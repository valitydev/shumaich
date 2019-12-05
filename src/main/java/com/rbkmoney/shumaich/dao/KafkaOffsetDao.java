package com.rbkmoney.shumaich.dao;

import com.rbkmoney.shumaich.domain.KafkaOffset;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.TopicPartition;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class KafkaOffsetDao {

    private static final String KEY = "kafka_offsets";

    // inject the template as ListOperations
    @Resource(name = "redisKafkaOffsetTemplate")
    private HashOperations<String, String, KafkaOffset> hashOperations;

    public List<KafkaOffset> loadOffsets(Collection<TopicPartition> topicPartitions) {
        List<KafkaOffset> kafkaOffsets = hashOperations.multiGet(KEY, convertToKeys(topicPartitions));
        kafkaOffsets.removeIf(Objects::isNull);
        return kafkaOffsets;
    }

    public void saveOffsets(List<KafkaOffset> kafkaOffsets) {
        hashOperations.putAll(KEY, convertToMap(kafkaOffsets));
    }

    private List<String> convertToKeys(Collection<TopicPartition> topicPartitions) {
        return topicPartitions.stream().map(TopicPartition::toString).collect(Collectors.toList());
    }

    private Map<String, KafkaOffset> convertToMap(List<KafkaOffset> kafkaOffsets) {
        return kafkaOffsets.stream().collect(
                Collectors.toMap(
                        kafkaOffset -> kafkaOffset.getTopicPartition().toString(),
                        kafkaOffset -> kafkaOffset
                )
        );
    }
}
