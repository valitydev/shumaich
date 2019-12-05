package com.rbkmoney.shumaich;

import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.domain.RequestLog;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

public class TestData {

    public static final String TEST_TOPIC = "test-topic";

    public static RequestLog requestLog() {
        return RequestLog.builder()
                .planId("plan")
                .operationType(OperationType.HOLD)
                .postingBatches(List.of())
                .build();
    }

    public static OperationLog operationLog() {
        return OperationLog.builder()

                .build();
    }

    public static KafkaOffset kafkaOffset(Integer partition, Long offset) {
        return KafkaOffset.builder()
                .topicPartition(new TopicPartition(TEST_TOPIC, partition))
                .offset(offset)
                .build();
    }

    public static TopicPartition topicPartition(int partition) {
        return new TopicPartition(TEST_TOPIC, partition);
    }
}
