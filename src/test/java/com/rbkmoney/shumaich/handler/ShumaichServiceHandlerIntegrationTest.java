package com.rbkmoney.shumaich.handler;

import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.IntegrationTestBase;
import com.rbkmoney.shumaich.dao.BalanceDao;
import com.rbkmoney.shumaich.dao.PlanDao;
import com.rbkmoney.shumaich.domain.Balance;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.exception.AccountsHaveDifferentCurrenciesException;
import com.rbkmoney.shumaich.exception.AccountsInPostingsAreEqualException;
import com.rbkmoney.shumaich.exception.CurrencyInPostingsNotConsistentException;
import com.rbkmoney.shumaich.exception.NotReadyException;
import com.rbkmoney.shumaich.helpers.TestData;
import com.rbkmoney.shumaich.helpers.TestUtils;
import com.rbkmoney.shumaich.kafka.TopicConsumptionManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDB;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Map;

import static com.rbkmoney.shumaich.helpers.TestData.MERCHANT_ACC;

@Slf4j
public class ShumaichServiceHandlerIntegrationTest extends IntegrationTestBase {

    @Autowired
    ShumaichServiceHandler handler;

    @Autowired
    BalanceDao balanceDao;

    @Autowired
    PlanDao planDao;

    @Autowired
    RocksDB rocksDB;

    @Autowired
    TopicConsumptionManager<String, OperationLog> operationLogTopicConsumptionManager;

    @Before
    public void clear() throws InterruptedException {
        operationLogTopicConsumptionManager.shutdownConsumers();
    }

    @After
    public void cleanup() throws IOException {
        folder.delete();
        folder.create();
    }

    @Test
    public void holdSuccess() throws TException {
        handler.hold(TestData.postingPlanChange(), null);

        Balance balance = balanceDao.getBalance(MERCHANT_ACC);

        Assert.assertNotNull(balance);
        Assert.assertEquals(0, balance.getAmount().intValue());
        Assert.assertEquals(-3, balance.getMinAmount().intValue());
        Assert.assertEquals(100, balance.getMaxAmount().intValue());


    }

    @Test
    public void holdIdempotency() throws TException {
        PostingPlanChange plan1 = TestData.postingPlanChange();
        handler.hold(plan1, null);
        handler.hold(plan1, null);
        handler.hold(plan1, null);

        PostingPlanChange plan2 = TestData.postingPlanChange();
        plan2.setId("plan2");
        handler.hold(plan2, null);
        handler.hold(plan2, null);
        handler.hold(plan2, null);

        Balance balance = balanceDao.getBalance(MERCHANT_ACC);

        Assert.assertNotNull(balance);
        Assert.assertEquals(0, balance.getAmount().intValue());
        Assert.assertEquals(-6, balance.getMinAmount().intValue());
        Assert.assertEquals(200, balance.getMaxAmount().intValue());
    }

    @Test
    public void holdWithClockRightInTime() throws TException {
        PostingPlanChange plan1 = TestData.postingPlanChange();
        Clock clock = handler.hold(plan1, null);

        //second batch
        plan1.getBatch().setId(2L);
        handler.hold(plan1, clock);

        Balance balance = balanceDao.getBalance(MERCHANT_ACC);

        Assert.assertNotNull(balance);
        Assert.assertEquals(0, balance.getAmount().intValue());
        Assert.assertEquals(-6, balance.getMinAmount().intValue());
        Assert.assertEquals(200, balance.getMaxAmount().intValue());
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

}
