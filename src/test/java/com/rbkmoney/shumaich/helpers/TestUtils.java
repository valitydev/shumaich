package com.rbkmoney.shumaich.helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.shumaich.converter.CommonConverter;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.service.ClockService;
import com.rbkmoney.shumaich.utils.VectorClockSerde;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestUtils {

    private static ClockService clockService = new ClockService(null);

    public static Clock moveClockFurther(Clock clock, Map<Long, Long> partitionsAndIncrement) {
        List<KafkaOffset> kafkaOffsets = clockService.parseClock(VectorClockSerde.deserialize(clock.getVector()));
        kafkaOffsets.forEach(kafkaOffset -> {
            Integer partition = kafkaOffset.getTopicPartition().partition();
            if (partitionsAndIncrement.containsKey(partition.longValue()))
                kafkaOffset.setOffset(kafkaOffset.getOffset() + partitionsAndIncrement.get(partition.longValue()));
        });
        return Clock.vector(
                VectorClockSerde.serialize(
                        CommonConverter.serialize(
                                new com.rbkmoney.shumaich.domain.Clock(kafkaOffsets.get(0).getTopicPartition().topic(),
                                kafkaOffsets
                                        .stream()
                                        .map(com.rbkmoney.shumaich.domain.Clock.PartitionOffsetPair::new)
                                        .collect(Collectors.toList())))
                )
        );
    }

    public static String createSerializedClock() {
        return CommonConverter.serialize(
                new com.rbkmoney.shumaich.domain.Clock("test",
                        List.of(new com.rbkmoney.shumaich.domain.Clock.PartitionOffsetPair(1, 1L),
                                new com.rbkmoney.shumaich.domain.Clock.PartitionOffsetPair(2, 2L))
                )
        );
    }

}
