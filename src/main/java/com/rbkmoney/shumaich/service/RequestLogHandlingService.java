package com.rbkmoney.shumaich.service;

import com.rbkmoney.shumaich.converter.RequestLogToOperationLogListConverter;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.RequestLog;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class RequestLogHandlingService implements Handler<RequestLog> {

    private final KafkaTemplate<Long, OperationLog> operationLogKafkaTemplate;
    private final RequestLogToOperationLogListConverter converter;

    @Override
    public void handle(ConsumerRecords<?, RequestLog> records) {
        for (ConsumerRecord<?, RequestLog> record : records) {
            List<OperationLog> operationLogs = converter.convert(record.value());
            for (OperationLog operationLog : operationLogs) {
                operationLogKafkaTemplate.sendDefault(operationLog.getAccount(), operationLog);
            }
        }
        operationLogKafkaTemplate.flush();
    }
}
