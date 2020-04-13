package com.rbkmoney.shumaich.handler;

import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.IntegrationTestBase;
import com.rbkmoney.shumaich.dao.BalanceDao;
import com.rbkmoney.shumaich.dao.PlanDao;
import com.rbkmoney.shumaich.domain.Balance;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.exception.*;
import com.rbkmoney.shumaich.helpers.TestData;
import com.rbkmoney.shumaich.helpers.TestUtils;
import com.rbkmoney.shumaich.kafka.TopicConsumptionManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;

import java.io.IOException;
import java.util.Map;

import static com.rbkmoney.shumaich.helpers.TestData.*;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;
import static org.hamcrest.CoreMatchers.notNullValue;

@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class ShumaichServiceHandlerIntegrationTest extends IntegrationTestBase {

    @Autowired
    ShumaichServiceHandler handler;

    @Autowired
    BalanceDao balanceDao;

    @Autowired
    PlanDao planDao;

    @Autowired
    TransactionDB rocksDB;

    @Autowired
    TopicConsumptionManager<String, OperationLog> operationLogTopicConsumptionManager;

    @Before
    public void clearDbData() throws RocksDBException {
        rocksDB.delete(planDao.getColumnFamilyHandle(), (PLAN_ID + "_HOLD").getBytes());
        rocksDB.delete(planDao.getColumnFamilyHandle(), "plan1_HOLD".getBytes());
        rocksDB.delete(planDao.getColumnFamilyHandle(), "plan2_HOLD".getBytes());
        rocksDB.delete(planDao.getColumnFamilyHandle(), (PLAN_ID + "_COMMIT").getBytes());
        rocksDB.delete(planDao.getColumnFamilyHandle(), "plan1_COMMIT".getBytes());
        rocksDB.delete(planDao.getColumnFamilyHandle(), "plan2_COMMIT".getBytes());
        rocksDB.delete(planDao.getColumnFamilyHandle(), (PLAN_ID + "_ROLLBACK").getBytes());
        rocksDB.delete(planDao.getColumnFamilyHandle(), "plan1_ROLLBACK".getBytes());
        rocksDB.delete(planDao.getColumnFamilyHandle(), "plan2_ROLLBACK".getBytes());
        rocksDB.delete(balanceDao.getColumnFamilyHandle(), MERCHANT_ACC.getBytes());
        rocksDB.delete(balanceDao.getColumnFamilyHandle(), SYSTEM_ACC.getBytes());
        rocksDB.delete(balanceDao.getColumnFamilyHandle(), PROVIDER_ACC.getBytes());
    }

    @Test
    public void holdSuccess() throws TException, InterruptedException, IOException {

        handler.hold(TestData.postingPlanChange(), null);

        await().untilAsserted(() -> {
            Balance balance = balanceDao.get(MERCHANT_ACC);

            Assert.assertNotNull(balance);
            Assert.assertEquals(0, balance.getAmount().intValue());
            Assert.assertEquals(-3, balance.getMinAmount().intValue());
            Assert.assertEquals(100, balance.getMaxAmount().intValue());
        });

    }

    @Test
    public void holdIdempotency() throws TException, InterruptedException {
        PostingPlanChange plan1 = TestData.postingPlanChange();
        handler.hold(plan1, null);
        handler.hold(plan1, null);
        handler.hold(plan1, null);

        PostingPlanChange plan2 = TestData.postingPlanChange();
        plan2.setId("plan2");
        handler.hold(plan2, null);
        handler.hold(plan2, null);
        handler.hold(plan2, null);

        await().untilAsserted(() -> {
            Balance balance = balanceDao.get(MERCHANT_ACC);
            Assert.assertNotNull(balance);
            Assert.assertEquals(0, balance.getAmount().intValue());
            Assert.assertEquals(-6, balance.getMinAmount().intValue());
            Assert.assertEquals(200, balance.getMaxAmount().intValue());
        });
    }

    @Test
    public void holdWithClockRightInTime() throws TException, InterruptedException {

        PostingPlanChange plan1 = TestData.postingPlanChange();
        Clock clock = handler.hold(plan1, null);

        //second batch
        plan1.getBatch().setId(2L);

        given().ignoreExceptions().await()
                .until(() -> handler.hold(plan1, clock), notNullValue());


        await().untilAsserted(() -> {
            Balance balance = balanceDao.get(MERCHANT_ACC);

            Assert.assertNotNull(balance);
            Assert.assertEquals(0, balance.getAmount().intValue());
            Assert.assertEquals(-6, balance.getMinAmount().intValue());
            Assert.assertEquals(200, balance.getMaxAmount().intValue());
        });
    }

    @Test(expected = NotReadyException.class)
    public void holdWithClockTooEarly() throws TException {
        PostingPlanChange plan1 = TestData.postingPlanChange();
        Clock clock = handler.hold(plan1, null);

        //second batch
        plan1.getBatch().setId(2L);

        clock = TestUtils.moveClockFurther(clock, Map.of(3L, 10L));
        handler.hold(plan1, clock);
    }

    @Test(expected = CurrencyInPostingsNotConsistentException.class)
    public void holdInvalidPostingsDifferentCurrency() throws TException {
        PostingPlanChange plan1 = TestData.postingPlanChange();
        plan1.getBatch().getPostings().get(0).setCurrencySymCode("USD");
        handler.hold(plan1, null);
    }

    @Test(expected = AccountsInPostingsAreEqualException.class)
    public void holdInvalidPostingsEqualAccs() throws TException {
        PostingPlanChange plan1 = TestData.postingPlanChange();
        plan1.getBatch().getPostings().get(0).setFromAccount(plan1.getBatch().getPostings().get(0).getToAccount());
        handler.hold(plan1, null);
    }

    @Test(expected = AccountsHaveDifferentCurrenciesException.class)
    public void holdInvalidPostingsDifferentAccountCurrencies() throws TException {
        PostingPlanChange plan1 = TestData.postingPlanChange();
        plan1.getBatch().getPostings().get(0).getFromAccount().setCurrencySymCode("USD");
        handler.hold(plan1, null);
    }

    @Test
    public void commitSuccess() throws TException {
        Clock holdClock = handler.hold(postingPlanChange(), null);

        await().until(() -> handler.commitPlan(TestData.postingPlan(), holdClock), notNullValue());

        await().untilAsserted(() -> {
            Balance balance = balanceDao.get(MERCHANT_ACC);

            Assert.assertNotNull(balance);
            Assert.assertEquals(97, balance.getAmount().intValue());
            Assert.assertEquals(97, balance.getMinAmount().intValue());
            Assert.assertEquals(97, balance.getMaxAmount().intValue());
        });
    }

    @Test(expected = HoldNotExistException.class)
    public void commitIdempotency() throws TException {
        Clock holdClock = handler.hold(postingPlanChange(), null);

        //wait for hold to be consumed
        await().until(() -> handler.commitPlan(TestData.postingPlan(), holdClock), notNullValue());
        
        await().untilAsserted(() -> {
            Balance balance = balanceDao.get(MERCHANT_ACC);
            Assert.assertNotNull(balance);
            Assert.assertEquals(97, balance.getAmount().intValue());
            Assert.assertEquals(97, balance.getMinAmount().intValue());
            Assert.assertEquals(97, balance.getMaxAmount().intValue());
        });
        
        handler.commitPlan(TestData.postingPlan(), holdClock);
        
    }
    @Test
    public void rollbackSuccess() throws TException {
        Clock holdClock = handler.hold(postingPlanChange(), null);

        await().until(() -> handler.rollbackPlan(TestData.postingPlan(), holdClock), notNullValue());

        await().untilAsserted(() -> {
            Balance balance = balanceDao.get(MERCHANT_ACC);

            Assert.assertNotNull(balance);
            Assert.assertEquals(0, balance.getAmount().intValue());
            Assert.assertEquals(0, balance.getMinAmount().intValue());
            Assert.assertEquals(0, balance.getMaxAmount().intValue());
        });
    }

    @Test(expected = HoldNotExistException.class)
    public void rollbackIdempotency() throws TException {
        Clock holdClock = handler.hold(postingPlanChange(), null);

        //wait for hold to be consumed
        await().until(() -> handler.rollbackPlan(TestData.postingPlan(), holdClock), notNullValue());

        await().untilAsserted(() -> {
            Balance balance = balanceDao.get(MERCHANT_ACC);

            Assert.assertNotNull(balance);
            Assert.assertEquals(0, balance.getAmount().intValue());
            Assert.assertEquals(0, balance.getMinAmount().intValue());
            Assert.assertEquals(0, balance.getMaxAmount().intValue());
        });

        handler.rollbackPlan(TestData.postingPlan(), holdClock);
    }

    @Test(expected = HoldChecksumMismatchException.class)
    public void finalOperationChecksumMismatch() throws TException {
        Clock holdClock = handler.hold(postingPlanChange(), null);

        PostingPlan postingPlan = postingPlan();
        Posting posting = postingPlan.getBatchList().get(0).getPostings().get(0);
        posting.setAmount(posting.getAmount() + 1);

        //wait for hold to be consumed
        await().ignoreExceptionsInstanceOf(NotReadyException.class)
                .until(() -> handler.rollbackPlan(postingPlan, holdClock), notNullValue());
    }

    @Test(expected = HoldNotExistException.class)
    public void finalOperationHoldNotExist() throws TException {
        Clock fakeClock = handler.hold(postingPlanChange(), null);

        PostingPlan postingPlan = postingPlan();
        postingPlan.setId("kekPlan");

        //wait for hold to be consumed
        await().ignoreExceptionsInstanceOf(NotReadyException.class)
                .until(() -> handler.rollbackPlan(postingPlan, fakeClock), notNullValue());
    }

    @Test(expected = HoldNotExistException.class)
    public void finalOperationBatchNotExist() throws TException {
        Clock fakeClock = handler.hold(postingPlanChange(), null);

        PostingPlan postingPlan = postingPlan();
        postingPlan.getBatchList().get(0).setId(999L);

        //wait for hold to be consumed
        await().ignoreExceptionsInstanceOf(NotReadyException.class)
                .until(() -> handler.rollbackPlan(postingPlan, fakeClock), notNullValue());
    }

}
