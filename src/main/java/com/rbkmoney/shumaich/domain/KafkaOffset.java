package com.rbkmoney.shumaich.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
public class KafkaOffset implements Serializable {
    private TopicPartition topicPartition;
    private Long offset;
}
