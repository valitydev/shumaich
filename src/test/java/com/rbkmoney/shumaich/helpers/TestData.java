package com.rbkmoney.shumaich.helpers;

import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingBatch;
import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.converter.PostingBatchDamselToPostingBatchConverter;
import com.rbkmoney.shumaich.converter.PostingDamselToPostingConverter;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.stream.Collectors;

public class TestData {

    public static final String TEST_TOPIC = "test_topic";
    public static final String OPERATION_LOG_TOPIC = "operation_log";
    public static final String PROVIDER_ACC = "1";
    public static final String SYSTEM_ACC = "2";
    public static final String MERCHANT_ACC = "3";
    public static final String PLAN_ID = "plan";

    private static PostingBatchDamselToPostingBatchConverter converter = new PostingBatchDamselToPostingBatchConverter(
            new PostingDamselToPostingConverter()
    );

    public static PostingPlanOperation postingPlanOperation(String planId) {
        return PostingPlanOperation.builder()
                .planId(planId)
                .operationType(OperationType.HOLD)
                .postingBatches(
                        List.of(
                                PostingGenerator.createBatch(PROVIDER_ACC, SYSTEM_ACC, MERCHANT_ACC),
                                PostingGenerator.createBatch(PROVIDER_ACC, SYSTEM_ACC, MERCHANT_ACC)
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
                                PostingGenerator.createBatch(PROVIDER_ACC, SYSTEM_ACC, MERCHANT_ACC),
                                PostingGenerator.createBatch(PROVIDER_ACC, SYSTEM_ACC, MERCHANT_ACC)
                        ).stream().map(converter::convert).collect(Collectors.toList())
                )
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
        return PostingGenerator.createPostingPlanChange(PLAN_ID, PROVIDER_ACC, SYSTEM_ACC, MERCHANT_ACC);
    }

    public static PostingPlan postingPlan() {
        return PostingGenerator.createPostingPlan(PLAN_ID, PROVIDER_ACC, SYSTEM_ACC, MERCHANT_ACC);
    }

    public static Posting postingDamsel() {
        return PostingGenerator.createPosting();
    }

    public static PostingBatch postingBatchDamsel() {
        return PostingGenerator.createBatch(PROVIDER_ACC, SYSTEM_ACC, MERCHANT_ACC);
    }
}
