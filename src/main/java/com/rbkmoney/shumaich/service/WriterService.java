package com.rbkmoney.shumaich.service;

import com.rbkmoney.shumaich.converter.PostingPlanOperationToOperationLogListConverter;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import com.rbkmoney.shumaich.exception.KafkaException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class WriterService {

    private final PostingPlanOperationToOperationLogListConverter converter;
    private final KafkaTemplate<String, OperationLog> kafkaTemplate;

    public List<RecordMetadata> write(PostingPlanOperation postingPlanOperation) {
        List<OperationLog> operationLogs = converter.convert(postingPlanOperation);
        List<ListenableFuture<SendResult<String, OperationLog>>> futures = new ArrayList<>();
        for (OperationLog operationLog : operationLogs) {
            futures.add(kafkaTemplate.sendDefault(operationLog.getAccount().getId(), operationLog));
        }
        try {
            ArrayList<RecordMetadata> recordMetadataList = new ArrayList<>();
            for (ListenableFuture<SendResult<String, OperationLog>> future : futures) {
                recordMetadataList.add(future.get().getRecordMetadata());
            }
            return recordMetadataList;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Write operation interrupted or answer from broker timeout, postingPlanOperation:{}",
                    postingPlanOperation, e);
            throw new KafkaException();
        }
    }

}
