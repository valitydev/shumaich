package com.rbkmoney.shumaich.service;

import com.rbkmoney.damsel.shumaich.OperationLog;
import com.rbkmoney.shumaich.converter.PostingPlanOperationToOperationLogListConverter;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import com.rbkmoney.shumaich.exception.KafkaException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class WriterService {

    private final PostingPlanOperationToOperationLogListConverter converter;
    private final KafkaTemplate<Long, OperationLog> kafkaTemplate;

    public List<RecordMetadata> write(PostingPlanOperation postingPlanOperation) {
        List<OperationLog> operationLogs = converter.convert(postingPlanOperation);
        List<ListenableFuture<SendResult<Long, OperationLog>>> futures = new ArrayList<>();
        for (OperationLog operationLog : operationLogs) {
            futures.add(kafkaTemplate.sendDefault(operationLog.getAccount().getId(), operationLog));
        }
        try {
            Map<TopicPartition, RecordMetadata> recordMetadataMap = new HashMap<>();
            for (ListenableFuture<SendResult<Long, OperationLog>> future : futures) {
                RecordMetadata recordMetadata = future.get().getRecordMetadata(); //todo completable future?
                TopicPartition topicPartition = new TopicPartition(recordMetadata.topic(), recordMetadata.partition());
                RecordMetadata mapRecordMetadata = recordMetadataMap.get(topicPartition);
                if (mapRecordMetadata == null || mapRecordMetadata.offset() < recordMetadata.offset()) {
                    recordMetadataMap.put(topicPartition, recordMetadata);
                }
            }
            return new ArrayList<>(recordMetadataMap.values());
        } catch (InterruptedException e) {
            log.error("Write operation interrupted, postingPlanOperation:{}", postingPlanOperation, e);
            Thread.currentThread().interrupt();
            throw new KafkaException();
        } catch (ExecutionException e) {
            log.error("Write operation - answer from broker timeout, postingPlanOperation:{}", postingPlanOperation, e);
            throw new KafkaException(e);
        }
    }

}
