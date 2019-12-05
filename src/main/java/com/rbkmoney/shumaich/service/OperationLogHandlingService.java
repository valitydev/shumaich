package com.rbkmoney.shumaich.service;

import com.rbkmoney.shumaich.domain.OperationLog;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OperationLogHandlingService implements Handler<OperationLog> {

    @Override
    public void handle(ConsumerRecords<?, OperationLog> records) {
        //todo
        records.forEach(record -> log.info(record.value().toString()));
    }
}
