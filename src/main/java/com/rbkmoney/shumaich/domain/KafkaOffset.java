package com.rbkmoney.shumaich.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.kafka.common.TopicPartition;

@Data
@Builder
@AllArgsConstructor
public class KafkaOffset {
    private TopicPartition topicPartition;
    private Long offset;
}
