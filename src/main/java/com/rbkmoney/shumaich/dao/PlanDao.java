package com.rbkmoney.shumaich.dao;


import com.rbkmoney.shumaich.converter.ByteArrayConverter;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.Plan;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashSet;
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
            if (plan == null) {
                return false;
            }
            if (plan.getSequenceArrived().contains(operationLog.getSequence())) {
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
                        .sequencesTotal(operationLog.getTotal())
                        .sequenceArrived(sequenceArrived)
                        .build()
        ));
    }

    private void addToPlan(Transaction transaction, OperationLog operationLog) throws RocksDBException {
        byte[] planBytes = transaction.getForUpdate(new ReadOptions(), columnFamilyHandle, getKey(operationLog), true);
        Plan plan = ByteArrayConverter.fromBytes(planBytes, Plan.class);
        plan.getSequenceArrived().add(operationLog.getSequence());
        transaction.put(columnFamilyHandle, plan.getPlanId().getBytes(), ByteArrayConverter.toBytes(plan));
    }

    private byte[] getKey(OperationLog operationLog) {
        return String.format("%s_%s", operationLog.getPlanId(), operationLog.getOperationType()).getBytes();
    }
}
