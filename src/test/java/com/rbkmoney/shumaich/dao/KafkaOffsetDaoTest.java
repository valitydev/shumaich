package com.rbkmoney.shumaich.dao;

import com.rbkmoney.shumaich.RocksdbTestBase;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.helpers.TestData;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.RocksDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

import java.io.IOException;
import java.util.List;

@Slf4j
@ContextConfiguration(classes = {KafkaOffsetDao.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class KafkaOffsetDaoTest extends RocksdbTestBase {

    @Autowired
    KafkaOffsetDao kafkaOffsetDao;

    @Autowired
    RocksDB rocksDB;

    @After
    public void cleanup() throws IOException {
        folder.delete();
        folder.create();
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

    @Test
    public void checkOffsetsOrder() {
        kafkaOffsetDao.saveOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 1, 10L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 10L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 10L)
        ));

        Assert.assertFalse(kafkaOffsetDao.isBeforeCurrentOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 1, 10L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 10L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 10L)
        )));

        Assert.assertTrue(kafkaOffsetDao.isBeforeCurrentOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 1, 9L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 6L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 1L)
        )));

        Assert.assertTrue(kafkaOffsetDao.isBeforeCurrentOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 1L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 6L)
        )));

        Assert.assertTrue(kafkaOffsetDao.isBeforeCurrentOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 6L)
        )));

        Assert.assertFalse(kafkaOffsetDao.isBeforeCurrentOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 10L)
        )));

        Assert.assertFalse(kafkaOffsetDao.isBeforeCurrentOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 15L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 15L)
        )));

        Assert.assertFalse(kafkaOffsetDao.isBeforeCurrentOffsets(List.of(
                TestData.kafkaOffset(TestData.TEST_TOPIC, 1, 1L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 2, 15L),
                TestData.kafkaOffset(TestData.TEST_TOPIC, 3, 15L)
        )));

    }

}
