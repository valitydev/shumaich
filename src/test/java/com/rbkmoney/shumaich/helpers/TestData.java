package com.rbkmoney.shumaich.helpers;

import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingBatch;
import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.converter.PostingBatchDamselToPostingBatchConverter;
import com.rbkmoney.shumaich.converter.PostingDamselToPostingConverter;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.stream.Collectors;

public class TestData {

    public static final String TEST_TOPIC = "test_topic";
    public static final String OPERATION_LOG_TOPIC = "operation_log";

    private static PostingBatchDamselToPostingBatchConverter converter = new PostingBatchDamselToPostingBatchConverter(
            new PostingDamselToPostingConverter()
    );

    public static PostingPlanOperation postingPlanOperation(String planId) {
        return PostingPlanOperation.builder()
                .planId(planId)
                .operationType(OperationType.HOLD)
                .postingBatches(
                        List.of(
                                PostingGenerator.createBatch(1L, 2L, 3L),
                                PostingGenerator.createBatch(1L, 2L, 3L)
                        ).stream().map(converter::convert).collect(Collectors.toList())
                )
                .build();
    }

    public static PostingPlanOperation postingPlanOperation() {
        return PostingPlanOperation.builder()
                .planId("plan")
                .operationType(OperationType.HOLD)
                .postingBatches(
                        List.of(
                                PostingGenerator.createBatch(1L, 2L, 3L),
                                PostingGenerator.createBatch(1L, 2L, 3L)
                        ).stream().map(converter::convert).collect(Collectors.toList())
                )
                .build();
    }

    public static OperationLog operationLog() {
        return OperationLog.builder()
                .build();
    }

    public static OperationLog operationLog(String planId) {
        return OperationLog.builder()
                .build();
    }

    public static KafkaOffset kafkaOffset(String topicName, Integer partition, Long offset) {
        return KafkaOffset.builder()
                .topicPartition(new TopicPartition(topicName, partition))
                .offset(offset)
                .build();
    }

    public static TopicPartition topicPartition(int partition) {
        return new TopicPartition(TEST_TOPIC, partition);
    }

    public static PostingPlanChange postingPlanChange() {
        return PostingGenerator.createPostingPlanChange("plan", 1L, 2L, 3L);
    }

    public static PostingPlan postingPlan() {
        return PostingGenerator.createPostingPlan("plan", 1L, 2L, 3L);
    }

    public static Posting postingDamsel() {
        return PostingGenerator.createPosting();
    }

    public static PostingBatch postingBatchDamsel() {
        return PostingGenerator.createBatch(1L, 2L, 3L);
    }
}
