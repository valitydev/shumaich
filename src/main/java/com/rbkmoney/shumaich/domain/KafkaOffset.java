package com.rbkmoney.shumaich.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.common.TopicPartition;

@Data
@AllArgsConstructor
public class KafkaOffset {

    private TopicPartition topicPartition;
    private Long offset;
}
