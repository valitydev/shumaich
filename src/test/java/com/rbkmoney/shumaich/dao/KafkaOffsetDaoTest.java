package com.rbkmoney.shumaich.dao;

import com.rbkmoney.shumaich.RedisTestBase;
import com.rbkmoney.shumaich.TestData;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.ContextConfiguration;

import java.util.List;
import java.util.Map;

@Slf4j
@ContextConfiguration(classes = {KafkaOffsetDao.class})
public class KafkaOffsetDaoTest extends RedisTestBase {

    @Autowired
    KafkaOffsetDao kafkaOffsetDao;

    @Autowired
    RedisTemplate<String, KafkaOffset> kafkaOffsetRedisTemplate;

    @After
    public void cleanUp() {
        HashOperations<String, Object, Object> hashOps = kafkaOffsetRedisTemplate.opsForHash();
        Map<Object, Object> entries = hashOps.entries("kafka_offsets");
        entries.replaceAll((key, value) -> null);
        hashOps.putAll("kafka_offsets", entries);
    }

    @Test
    public void saveAndLoad() {
        kafkaOffsetDao.saveOffsets(List.of(
                TestData.kafkaOffset(1, 1L),
                TestData.kafkaOffset(2, 1L),
                TestData.kafkaOffset(3, 1L)
                ));

        List<KafkaOffset> kafkaOffsets = kafkaOffsetDao.loadOffsets(List.of(
                TestData.topicPartition(1),
                TestData.topicPartition(2),
                TestData.topicPartition(3)
        ));

        kafkaOffsets.forEach(offset -> Assert.assertEquals(1L, offset.getOffset().longValue()));
    }

    @Test
    public void loadNotExistedOffsets() {
        List<KafkaOffset> kafkaOffsets = kafkaOffsetDao.loadOffsets(List.of(
                TestData.topicPartition(1),
                TestData.topicPartition(2),
                TestData.topicPartition(3)
        ));

        Assert.assertEquals(0, kafkaOffsets.size());
    }

    @Test
    public void rewriteExistingOffsets() {
        kafkaOffsetDao.saveOffsets(List.of(
                TestData.kafkaOffset(1, 1L),
                TestData.kafkaOffset(2, 1L),
                TestData.kafkaOffset(3, 1L)
        ));

        kafkaOffsetDao.saveOffsets(List.of(
                TestData.kafkaOffset(1, 10L),
                TestData.kafkaOffset(2, 10L),
                TestData.kafkaOffset(3, 10L)
        ));

        List<KafkaOffset> kafkaOffsets = kafkaOffsetDao.loadOffsets(List.of(
                TestData.topicPartition(1),
                TestData.topicPartition(2),
                TestData.topicPartition(3)
        ));

        kafkaOffsets.forEach(offset -> Assert.assertEquals(10L, offset.getOffset().longValue()));

    }

}
