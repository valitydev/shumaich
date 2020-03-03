package com.rbkmoney.shumaich.service;

import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.shumaich.dao.KafkaOffsetDao;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.exception.NotReadyException;
import com.rbkmoney.shumaich.utils.VectorClockSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClockService {

    private final static String DELIMITER = "@";

    private final KafkaOffsetDao kafkaOffsetDao;

    public String formClock(List<RecordMetadata> recordMetadataList) {
        return recordMetadataList.get(0).topic() + DELIMITER + recordMetadataList.stream()
                .map(recordMetadata -> String.format("%d%s%d", recordMetadata.partition(), DELIMITER, recordMetadata.offset()))
                .collect(Collectors.joining(DELIMITER));
    }

    public List<KafkaOffset> parseClock(String clock) {
        if (isBlank(clock)) {
            return Collections.emptyList();
        }
        String[] strings = clock.split(DELIMITER);
        String topicName = strings[0];
        ArrayList<KafkaOffset> kafkaOffsets = new ArrayList<>();
        for (int i = 1; i < strings.length; i += 2) {
            kafkaOffsets.add(new KafkaOffset(
                    new TopicPartition(topicName, Integer.parseInt(strings[i])),
                    Long.parseLong(strings[i + 1])
            ));
        }
        return kafkaOffsets;
    }

    public void checkClockTimeline(Clock clock, boolean canBeLatestOrEmpty) {
        if (clock == null || clock.isSetLatest()) {
            if (canBeLatestOrEmpty) {
                return;
            } else {
                throw new IllegalArgumentException("Clock can't be latest");
            }
        }

        List<KafkaOffset> kafkaOffsets = parseClock(VectorClockSerde.deserialize(clock.getVector()));

        if (kafkaOffsets.isEmpty()) {
            if (canBeLatestOrEmpty) {
                return;
            } else {
                throw new IllegalArgumentException("Clock can't be empty");
            }
        }

        if (!kafkaOffsetDao.isBeforeCurrentOffsets(kafkaOffsets)) {
            throw new NotReadyException();
        }

    }
}
