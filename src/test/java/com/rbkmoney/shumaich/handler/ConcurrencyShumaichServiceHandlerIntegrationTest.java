package com.rbkmoney.shumaich.handler;

import com.rbkmoney.damsel.shumpune.Balance;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.IntegrationTestBase;
import com.rbkmoney.shumaich.dao.BalanceDao;
import com.rbkmoney.shumaich.dao.PlanDao;
import com.rbkmoney.shumaich.domain.Account;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.helpers.HellgateClientExecutor;
import com.rbkmoney.shumaich.helpers.HoldPlansExecutor;
import com.rbkmoney.shumaich.helpers.PostingGenerator;
import com.rbkmoney.shumaich.kafka.TopicConsumptionManager;
import com.rbkmoney.shumaich.service.BalanceService;
import com.rbkmoney.shumaich.service.PlanService;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.rocksdb.TransactionDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.AlwaysRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.shaded.com.google.common.util.concurrent.Futures;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.rbkmoney.shumaich.helpers.TestData.*;
import static org.junit.Assert.assertEquals;

@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@Ignore
public class ConcurrencyShumaichServiceHandlerIntegrationTest extends IntegrationTestBase {

    private static final int ITERATIONS = 10;
    private static final int OPERATIONS = 15000;
    private static final int THREAD_NUM = 16;
    private static final long HOLD_AMOUNT = 100;

    public static final String TEST_CASE_FIRST = "test1";
    public static final String TEST_CASE_SECOND = "test2";

    @Autowired
    ShumaichServiceHandler serviceHandler;

    @Autowired
    TopicConsumptionManager<String, OperationLog> operationLogTopicConsumptionManager;

    @Autowired
    ApplicationContext applicationContext;

    @Autowired
    BalanceDao balanceDao;

    @Autowired
    BalanceService balanceService;

    @Autowired
    PlanService planService;

    @Autowired
    PlanDao planDao;

    @Autowired
    TransactionDB rocksDB;

    RetryTemplate retryTemplate = getRetryTemplate();

    @NotNull
    private RetryTemplate getRetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(new AlwaysRetryPolicy());
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(100L);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        return retryTemplate;
    }

    private ExecutorService executorService;

    @Test
    public void concurrentHoldsConsistencyTest() throws InterruptedException {
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            executorService = Executors.newFixedThreadPool(THREAD_NUM);

            List<Future<Map.Entry<String, Balance>>> futureList = new ArrayList<>();

            for (int operation = 0; operation < OPERATIONS; operation++) {
                PostingPlanChange postingPlanChange = PostingGenerator.createPostingPlanChange(
                        TEST_CASE_FIRST + "_iteration" + iteration + "_operation" + operation,
                         TEST_CASE_FIRST + "_iteration" + iteration + PROVIDER_ACC,
                         TEST_CASE_FIRST + "_iteration" + iteration + SYSTEM_ACC,
                         TEST_CASE_FIRST + "_iteration" + iteration + MERCHANT_ACC,
                        HOLD_AMOUNT);

                futureList.add(executorService.submit(new HoldPlansExecutor(
                        serviceHandler,
                        postingPlanChange,
                        retryTemplate,
                        TEST_CASE_FIRST + "_iteration" + iteration + MERCHANT_ACC)
                ));
            }

            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.HOURS);

            Balance balance = futureList.stream()
                    .map(Futures::getUnchecked)
                    .map(Map.Entry::getValue)
                    .min(Comparator.comparing(Balance::getMinAvailableAmount))
                    .get();

            long expectedBalance = -HOLD_AMOUNT * OPERATIONS;
            assertEquals("Wrong balance after holds", expectedBalance, balance.getMinAvailableAmount());
        }
    }

    @Test
    public void concurrentHellgateSimulationTest() throws InterruptedException {
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            executorService = Executors.newFixedThreadPool(THREAD_NUM);

            List<Future<Map.Entry<String, Balance>>> futureList = new ArrayList<>();

            initBalance(TEST_CASE_SECOND + "_iteration" + iteration + MERCHANT_ACC);

            for (int operation = 0; operation < OPERATIONS; operation += 2) {
                futureList.add(executorService.submit(new HellgateClientExecutor(
                        serviceHandler,
                        PostingGenerator.createPostingPlanChangeTwoAccs(
                                TEST_CASE_SECOND + "_iteration" + iteration + "_operation" + operation,
                                TEST_CASE_SECOND + "_iteration" + iteration + SYSTEM_ACC,
                                TEST_CASE_SECOND + "_iteration" + iteration + MERCHANT_ACC,
                                HOLD_AMOUNT),
                        retryTemplate,
                        TEST_CASE_SECOND + "_iteration" + iteration + SYSTEM_ACC)
                ));
                futureList.add(executorService.submit(new HellgateClientExecutor(
                        serviceHandler,
                        PostingGenerator.createPostingPlanChangeTwoAccs(
                                TEST_CASE_SECOND + "_iteration" + iteration + "_operation" + (operation + 1),
                                TEST_CASE_SECOND + "_iteration" + iteration + MERCHANT_ACC,
                                TEST_CASE_SECOND + "_iteration" + iteration + SYSTEM_ACC,
                                HOLD_AMOUNT),
                        retryTemplate,
                        TEST_CASE_SECOND + "_iteration" + iteration + MERCHANT_ACC)
                ));
            }

            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.HOURS);

            checkBalancesAreNotNegative(futureList);
        }
    }

    private void initBalance(String account) {
        balanceService.createNewBalance(new Account(account, "RUB"));
        balanceService.proceedHold(OperationLog.builder()
                .planId("test")
                .account(new Account(account, "RUB"))
                .amountWithSign(HOLD_AMOUNT)
                .currencySymbolicCode("RUB")
                .sequence(1L)
                .total(1L)
                .batchId(1L)
                .operationType(OperationType.HOLD)
                .build());
        balanceService.proceedFinalOp(OperationLog.builder()
                .planId("test")
                .account(new Account(account, "RUB"))
                .amountWithSign(HOLD_AMOUNT)
                .currencySymbolicCode("RUB")
                .sequence(1L)
                .total(1L)
                .batchId(1L)
                .operationType(OperationType.COMMIT)
                .build());
    }

    private void checkBalancesAreNotNegative(List<Future<Map.Entry<String, Balance>>> futureList) {
        Assert.assertFalse(futureList.stream()
                .map(Futures::getUnchecked)
                .map(Map.Entry::getValue)
                .anyMatch(balance -> balance.getOwnAmount() < 0));
    }

}
