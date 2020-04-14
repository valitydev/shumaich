package com.rbkmoney.shumaich.service;

import com.rbkmoney.shumaich.dao.PlanDao;
import com.rbkmoney.shumaich.domain.*;
import com.rbkmoney.shumaich.exception.HoldChecksumMismatchException;
import com.rbkmoney.shumaich.exception.HoldNotExistException;
import com.rbkmoney.shumaich.utils.HashUtils;
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

    public void checkIfHoldAndChecksumMatch(PostingPlanOperation postingPlanOperation) {
        Plan plan = planDao.get(getKeyForPlan(postingPlanOperation.getPlanId(), OperationType.HOLD));
        if (plan == null) {
            throw new HoldNotExistException();
        }

        for (PostingBatch postingBatch : postingPlanOperation.getPostingBatches()) {
            PlanBatch storedBatch = plan.getBatch(postingBatch.getId());
            if (storedBatch == null) {
                throw new HoldNotExistException();
            }
            if (!HashUtils.areHashesEqual(postingBatch.getPostings(), storedBatch.getBatchHash())) {
                throw new HoldChecksumMismatchException();
            }
        }
    }


    public boolean isFinished(OperationLog operationLog) {
        Plan plan = planDao.get(getKey(operationLog));
        PlanBatch batch = plan.getBatch(operationLog.getBatchId());
        return batch.isCompleted();
    }

    public void deletePlan(String planId) {
        planDao.delete(getKeyForPlan(planId, OperationType.HOLD));
        planDao.delete(getKeyForPlan(planId, OperationType.COMMIT));
        planDao.delete(getKeyForPlan(planId, OperationType.ROLLBACK));
    }

    private void createPlan(Transaction transaction, OperationLog operationLog) {
        Set<Long> sequencesArrived = new HashSet<>();
        sequencesArrived.add(operationLog.getSequence());

        planDao.putInTransaction(transaction, getKey(operationLog), Plan.builder()
                .planId(operationLog.getPlanId())
                .batches(Map.of(operationLog.getBatchId(), new PlanBatch(sequencesArrived, operationLog.getTotal(), operationLog.getBatchHash())))
                .build());
    }

    private void addToPlan(Transaction transaction, OperationLog operationLog, Plan plan) {
        PlanBatch batch = plan.getBatch(operationLog.getBatchId());
        if (batch == null) {
            batch = plan.addBatch(operationLog.getBatchId(), new PlanBatch(new HashSet<>(), operationLog.getTotal(), operationLog.getBatchHash()));
        }
        batch.addSequence(operationLog.getSequence());
        planDao.putInTransaction(transaction, getKey(operationLog), plan);
    }

    private String getKey(OperationLog operationLog) {
        return String.format("%s_%s", operationLog.getPlanId(), operationLog.getOperationType().toString());
    }

    private String getKeyForPlan(String planId, OperationType operationType) {
        return String.format("%s_%s", planId, operationType);
    }
}
