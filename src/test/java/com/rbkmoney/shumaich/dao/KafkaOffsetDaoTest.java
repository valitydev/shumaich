package com.rbkmoney.shumaich.dao;

import com.rbkmoney.shumaich.RedisTestBase;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.helpers.TestData;
import com.rbkmoney.shumaich.helpers.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.ContextConfiguration;

import java.util.List;

@Slf4j
@ContextConfiguration(classes = {KafkaOffsetDao.class})
public class KafkaOffsetDaoTest extends RedisTestBase {

    @Autowired
    KafkaOffsetDao kafkaOffsetDao;

    @Autowired
    RedisTemplate<String, KafkaOffset> kafkaOffsetRedisTemplate;

    @After
    public void cleanUp() throws InterruptedException {
        TestUtils.deleteOffsets(kafkaOffsetRedisTemplate);
    }

    @Test
    public void saveAndLoad() {
        kafkaOffsetDao.saveOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 1, 1L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 1L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 1L)
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
                TestData.kafkaOffset(TestData.TEST_TOPIC, 1, 1L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 1L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 1L)
        ));

        kafkaOffsetDao.saveOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 1, 10L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 10L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 10L)
        ));

        List<KafkaOffset> kafkaOffsets = kafkaOffsetDao.loadOffsets(List.of(
                TestData.topicPartition(1),
                TestData.topicPartition(2),
                TestData.topicPartition(3)
        ));

        kafkaOffsets.forEach(offset -> Assert.assertEquals(10L, offset.getOffset().longValue()));

    }

}
