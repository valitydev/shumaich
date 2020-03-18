package com.rbkmoney.shumaich.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.damsel.shumpune.LatestClock;
import com.rbkmoney.shumaich.dao.KafkaOffsetDao;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.exception.NotReadyException;
import com.rbkmoney.shumaich.helpers.TestUtils;
import com.rbkmoney.shumaich.utils.VectorClockSerde;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClockServiceTest {

    KafkaOffsetDao kafkaOffsetDao = mock(KafkaOffsetDao.class);
    ClockService service = new ClockService(kafkaOffsetDao);

    @Test
    public void successPath() {
        List<RecordMetadata> recordMetadata = List.of(getRecordMetadata(1, 10),
                getRecordMetadata(2, 7),
                getRecordMetadata(3, 25)
        );

        List<KafkaOffset> kafkaOffsets = service.parseClock(service.formClock(recordMetadata));
        kafkaOffsets.sort(Comparator.comparing(kafkaOffset -> kafkaOffset.getTopicPartition().partition()));

        assertOffsetsEquals(recordMetadata, kafkaOffsets);
    }

    @Test
    public void emptyClock() {
        Assert.assertTrue(service.parseClock("").isEmpty());
    }

    @Test
    public void checkClockTimeline_success() {
        when(kafkaOffsetDao.isBeforeCurrentOffsets(any())).thenReturn(true);
        service.checkClockTimeline(getClock(false), false);
        service.checkClockTimeline(getClock(false), true);
        service.checkClockTimeline(getClock(true), true);
        service.checkClockTimeline(getLatestClock(), true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkClockTimeline_latest() {
        service.checkClockTimeline(getLatestClock(), false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkClockTimeline_empty() {
        service.checkClockTimeline(getClock(true), false);
    }

    @Test(expected = NotReadyException.class)
    public void checkClockTimeline_notReady() {
        when(kafkaOffsetDao.isBeforeCurrentOffsets(any())).thenReturn(false);
        service.checkClockTimeline(getClock(false), false);
    }

    private RecordMetadata getRecordMetadata(Integer partition, Integer offset) {
        return new RecordMetadata(
                new TopicPartition("test", partition),
                0,
                offset,
                0,
                0L,
                0,
                0
        );
    }

    private void assertOffsetsEquals(List<RecordMetadata> recordMetadataList, List<KafkaOffset> kafkaOffsets) {
        for (int i = 0; i < kafkaOffsets.size(); i++) {
            KafkaOffset kafkaOffset = kafkaOffsets.get(i);
            RecordMetadata recordMetadata = recordMetadataList.get(i);
            Assert.assertEquals(kafkaOffset.getOffset().longValue(), recordMetadata.offset());
            Assert.assertEquals(kafkaOffset.getTopicPartition().topic(), recordMetadata.topic());
            Assert.assertEquals(kafkaOffset.getTopicPartition().partition(), recordMetadata.partition());
        }
    }

    private Clock getLatestClock() {
        return Clock.latest(new LatestClock());
    }

    private Clock getClock(boolean empty) {
        return empty ? Clock.vector(VectorClockSerde.serialize(""))
                : Clock.vector(VectorClockSerde.serialize(TestUtils.createSerializedClock()));
    }
}
