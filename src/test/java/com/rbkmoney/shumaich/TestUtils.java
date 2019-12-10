package com.rbkmoney.shumaich;

import com.rbkmoney.shumaich.domain.KafkaOffset;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Map;

public class TestUtils {

    public static void deleteOffsets(RedisTemplate<String, KafkaOffset> kafkaOffsetRedisTemplate) throws InterruptedException {
        HashOperations<String, Object, Object> hashOps = kafkaOffsetRedisTemplate.opsForHash();
        Map<Object, Object> entries = hashOps.entries("kafka_offsets");
        entries.replaceAll((key, value) -> null);
        hashOps.putAll("kafka_offsets", entries);
        Thread.sleep(300);
    }

}
