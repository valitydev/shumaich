package com.rbkmoney.shumaich.proofofconcept;

import com.rbkmoney.shumaich.proofofconcept.domain.Balance;
import com.rbkmoney.shumaich.proofofconcept.domain.BalanceNotReadyException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static com.rbkmoney.shumaich.domain.OperationType.COMMIT;
import static com.rbkmoney.shumaich.domain.OperationType.HOLD;

@Slf4j
public class ProofOfConceptTest {

    private Server server;

    @Before
    public void init() {
        server = new Server();
    }

    @Test
    public void testSimpleFlow() {
        server.initAccs(Utils.createAccounts());

        List<Integer> clock = server.makeOperation(Utils.createPlan("1", HOLD, 1));

        server.calculateBalancesToLatest();

        Utils.checkBalance(server.getBalance(1, clock), 0, -6800, 80000);
    }

    @Test
    public void testSimpleFlowWithRepeatedRecords() {
        server.initAccs(Utils.createAccounts());

        List<Integer> clock = server.makeOperation(Utils.createPlan("1", HOLD, 1));
        server.makeOperation(Utils.createPlan("1", HOLD, 1));

        server.calculateBalancesToLatest();

        Utils.checkBalance(server.getBalance(1, clock), 0, -6800, 80000);
    }

    @Test
    public void testTwoPlansWithRepeatedRecords() {
        server.initAccs(Utils.createAccounts());

        List<Integer> clock1 = server.makeOperation(Utils.createPlan("1", HOLD, 1));
        List<Integer> clock2 = server.makeOperation(Utils.createPlan("2", HOLD, 1));
        server.makeOperation(Utils.createPlan("1", HOLD, 1));
        server.makeOperation(Utils.createPlan("2", HOLD, 1));

        server.calculateBalancesToLatest();

        Utils.checkBalance(server.getBalance(1, clock1), 0, -6800 * 2, 80000 * 2);
        Utils.checkBalance(server.getBalance(1, clock2), 0, -6800 * 2, 80000 * 2);
    }

    @Test
    public void simpleFlowWithCommit() {
        server.initAccs(Utils.createAccounts());

        server.makeOperation(Utils.createPlan("1", HOLD, 1));
        List<Integer> clock = server.makeOperation(Utils.createPlan("1", COMMIT, 1));

        server.calculateBalancesToLatest();

        Utils.checkBalance(server.getBalance(1, clock), 73200, 73200, 73200);
    }

    @Test
    public void simpleFlowWithCommitWithRepeatedRecords() {
        server.initAccs(Utils.createAccounts());

        server.makeOperation(Utils.createPlan("1", HOLD, 1));
        server.makeOperation(Utils.createPlan("1", COMMIT, 1));
        List<Integer> clock = server.makeOperation(Utils.createPlan("1", COMMIT, 1));

        server.calculateBalancesToLatest();

        Utils.checkBalance(server.getBalance(1, clock), 73200, 73200, 73200);
    }

    @Test(expected = BalanceNotReadyException.class)
    public void balanceIsNotReady() {
        server.initAccs(Utils.createAccounts());

        List<Integer> clock = server.makeOperation(Utils.createPlan("1", HOLD, 1));

        Utils.checkBalance(server.getBalance(1, clock), 0, -6800, 80000);
    }

    @Test
    public void balanceIsReadyForOneOfAccounts() {
        server.initAccs(Utils.createAccounts());

        List<Integer> clock = server.makeOperation(Utils.createPlan("1", HOLD, 1));

        server.calculateBalancesTo(List.of(0, 5, 0));

        Utils.checkBalance(server.getBalance(1, clock), 0, -6800, 80000);
    }

    @Test(expected = BalanceNotReadyException.class)
    public void balanceIsNotReadyForOneOfAccounts() {
        server.initAccs(Utils.createAccounts());

        List<Integer> clock = server.makeOperation(Utils.createPlan("1", HOLD, 1));

        server.calculateBalancesTo(List.of(0, 5, 0));

        server.getBalance(2, clock);
    }

    @Test
    public void cantGetRealBalanceToNegative() {
        server.initAcc(0, new Balance(100, 100, 100));
        server.initAcc(1, new Balance(0, 0, 0));

        new Client(server, 0, 1, 100, "1").run();
        new Client(server, 0, 1, 100, "2").run();
        new Client(server, 0, 1, 100, "3").run();

        server.calculateBalancesToLatest();

        Assert.assertTrue(server.getBalance(0).getOwnAmount().get() >= 0);
    }


    @Test
    public void concurrentCircularPayments() throws InterruptedException {
        server.initAcc(0, new Balance(100, 100, 100));
        server.initAcc(1, new Balance(0, 0, 0));

        ExecutorService executorService = Executors.newFixedThreadPool(16);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            futures.add(executorService.submit(new Client(server, 0, 1, 100, "a" + i)));
            futures.add(executorService.submit(new Client(server, 1, 0, 100, "b" + i)));
        }

        futures.forEach(future -> {
            try {
                future.get(1, TimeUnit.MINUTES);
            } catch (InterruptedException | TimeoutException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        server.calculateBalancesToLatest();

        Balance balance1 = server.getBalance(0);
        Balance balance2 = server.getBalance(1);


        if (balance1.getOwnAmount().get() == 0) {
            Utils.checkBalance(balance1, 0, 0, 0);
            Utils.checkBalance(balance2, 100, 100, 100);
        } else {
            Utils.checkBalance(balance2, 0, 0, 0);
            Utils.checkBalance(balance1, 100, 100, 100);
        }
    }

}
