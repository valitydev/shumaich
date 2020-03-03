package com.rbkmoney.shumaich.dao;


import com.rbkmoney.shumaich.converter.ByteArrayConverter;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.Plan;
import com.rbkmoney.shumaich.domain.PlanBatch;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class PlanDao {

    private final static String COLUMN_FAMILY_NAME = "plan";
    private ColumnFamilyHandle columnFamilyHandle;

    private final RocksDB rocksDB;

    @PostConstruct
    public void initializeColumnFamily() throws RocksDBException {
        this.columnFamilyHandle = rocksDB.createColumnFamily(new ColumnFamilyDescriptor(COLUMN_FAMILY_NAME.getBytes()));
    }

    @PreDestroy
    public void closeColumnFamily() {
        this.columnFamilyHandle.close();
    }

    public void planModificationReceived(Transaction transaction, OperationLog operationLog) {
        try {
            byte[] planBytes = transaction.getForUpdate(new ReadOptions(), columnFamilyHandle,
                    getKey(operationLog), true);
            if (planBytes == null) {
                createPlan(transaction, operationLog);
            } else {
                addToPlan(transaction, operationLog);
            }
        } catch (RocksDBException e) {
            //todo log
            log.error("Error in transaction");
            e.printStackTrace();
        }
    }

    public boolean operationLogExists(OperationLog operationLog) {
        try {
            byte[] planBytes = rocksDB.get(columnFamilyHandle, getKey(operationLog));
            Plan plan = ByteArrayConverter.fromBytes(planBytes, Plan.class);
            if (plan == null || plan.getBatch(operationLog.getBatchId()) == null) {
                return false;
            }
            if (plan.getBatch(operationLog.getBatchId()).containsSequenceValue(operationLog.getSequence())) {
                return true;
            }
        } catch (RocksDBException e) {
            //todo log
            log.error("Error in plan");
            e.printStackTrace();
        }
        return false;
    }

    private void createPlan(Transaction transaction, OperationLog operationLog) throws RocksDBException {
        Set<Long> sequenceArrived = new HashSet<>();
        sequenceArrived.add(operationLog.getSequence());
        transaction.put(columnFamilyHandle, getKey(operationLog), ByteArrayConverter.toBytes(
                Plan.builder()
                        .planId(operationLog.getPlanId())
                        .batches(Map.of(operationLog.getBatchId(), new PlanBatch(sequenceArrived, operationLog.getTotal())))
                        .build()
        ));
    }

    private void addToPlan(Transaction transaction, OperationLog operationLog) throws RocksDBException {
        byte[] planBytes = transaction.getForUpdate(new ReadOptions(), columnFamilyHandle, getKey(operationLog), true);
        Plan plan = ByteArrayConverter.fromBytes(planBytes, Plan.class);
        PlanBatch batch = plan.getBatch(operationLog.getBatchId());
        if (batch == null) {
            batch = plan.addBatch(operationLog.getBatchId(),
                    new PlanBatch(new HashSet<>(), operationLog.getTotal()));
        }
        batch.addSequence(operationLog.getSequence());
        transaction.put(columnFamilyHandle, getKey(operationLog), ByteArrayConverter.toBytes(plan));
    }

    private byte[] getKey(OperationLog operationLog) {
        return getKey(operationLog.getPlanId(), operationLog.getOperationType().toString());
    }

    private byte[] getKey(String planId, String operationType) {
        return String.format("%s_%s", planId, operationType).getBytes();
    }
}
