package com.rbkmoney.shumaich.helpers;

import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.service.ClockService;
import com.rbkmoney.shumaich.utils.VectorClockSerde;

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
                        kafkaOffsets.get(0).getTopicPartition().topic() + "@" + kafkaOffsets.stream()
                                .map(kafkaOffset ->
                                        String.format("%d@%d", kafkaOffset.getTopicPartition().partition(),
                                                kafkaOffset.getOffset()))
                                .collect(Collectors.joining("@"))
                )
        );
    }

}
