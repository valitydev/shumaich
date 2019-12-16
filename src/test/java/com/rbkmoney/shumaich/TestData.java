package com.rbkmoney.shumaich;

import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.domain.RequestLog;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

public class TestData {

    public static final String TEST_TOPIC = "test_topic";
    public static final String OPERATION_LOG_TOPIC = "operation_log";
    public static final String REQUEST_LOG_TOPIC = "request_log";

    public static RequestLog requestLog() {
        return RequestLog.builder()
                .planId("plan")
                .operationType(OperationType.HOLD)
                .postingBatches(
                        List.of(
                                PostingGenerator.createBatch(1L, 2L, 3L),
                                PostingGenerator.createBatch(1L, 2L, 3L)
                        )
                )
                .build();
    }

    public static OperationLog operationLog() {
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
}
