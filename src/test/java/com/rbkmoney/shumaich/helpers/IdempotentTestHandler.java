package com.rbkmoney.shumaich.helpers;

import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.service.Handler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class IdempotentTestHandler implements Handler<OperationLog> {

    Map<String, Set<Long>> receivedRecords = new HashMap<>();

    @Override
    public void handle(ConsumerRecords<?, OperationLog> records) {
        for (ConsumerRecord<?, OperationLog> record : records) {
            OperationLog value = record.value();
            receivedRecords.putIfAbsent(value.getPlanId(), new HashSet<>());
            receivedRecords.get(value.getPlanId()).add(value.getSequence());
        }
    }

    public Integer countReceivedRecords(String planId) {
        return receivedRecords.get(planId).size();
    }
}
