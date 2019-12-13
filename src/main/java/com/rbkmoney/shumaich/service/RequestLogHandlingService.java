package com.rbkmoney.shumaich.service;

import com.rbkmoney.shumaich.domain.RequestLog;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RequestLogHandlingService implements Handler<RequestLog> {

    @Override
    public void handle(ConsumerRecords<?, RequestLog> records) {
        //todo
        records.forEach(record -> log.info(record.value().toString()));
    }
}
