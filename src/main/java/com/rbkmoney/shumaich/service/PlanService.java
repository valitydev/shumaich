package com.rbkmoney.shumaich.service;

import com.rbkmoney.shumaich.dao.PlanDao;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.Plan;
import com.rbkmoney.shumaich.domain.PlanBatch;
import lombok.RequiredArgsConstructor;
import org.rocksdb.Transaction;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;


@Service
@RequiredArgsConstructor
public class PlanService {

    private final PlanDao planDao;

    public boolean operationLogExists(OperationLog operationLog) {
        Plan plan = planDao.get(getKey(operationLog));
        return plan != null
                && plan.getBatch(operationLog.getBatchId()) != null
                && plan.getBatch(operationLog.getBatchId()).containsSequenceValue(operationLog.getSequence());
    }

    public void processPlanModification(Transaction transaction, OperationLog operationLog) {
        Plan plan = planDao.getForUpdate(transaction, getKey(operationLog));
        if (plan == null) {
            createPlan(transaction, operationLog);
        } else {
            addToPlan(transaction, operationLog, plan);
        }
    }


    private void createPlan(Transaction transaction, OperationLog operationLog) {
        Set<Long> sequencesArrived = new HashSet<>();
        sequencesArrived.add(operationLog.getSequence());

        planDao.putInTransaction(transaction, getKey(operationLog), Plan.builder()
                .planId(operationLog.getPlanId())
                .batches(Map.of(operationLog.getBatchId(), new PlanBatch(sequencesArrived, operationLog.getTotal())))
                .build());
    }

    private void addToPlan(Transaction transaction, OperationLog operationLog, Plan plan) {
        PlanBatch batch = plan.getBatch(operationLog.getBatchId());
        if (batch == null) {
            batch = plan.addBatch(operationLog.getBatchId(), new PlanBatch(new HashSet<>(), operationLog.getTotal()));
        }
        batch.addSequence(operationLog.getSequence());
        planDao.putInTransaction(transaction, getKey(operationLog), plan);
    }

    private String getKey(OperationLog operationLog) {
        return String.format("%s_%s", operationLog.getPlanId(), operationLog.getOperationType().toString());
    }
}
