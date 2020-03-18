package com.rbkmoney.shumaich.dao;

import com.rbkmoney.shumaich.converter.CommonConverter;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.rocksdb.*;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaOffsetDao extends RocksDbDao {

    private final static String COLUMN_FAMILY_NAME = "kafkaOffset";

    @Override
    public byte[] getColumnFamilyName() {
        return COLUMN_FAMILY_NAME.getBytes();
    }

    @PreDestroy
    public void destroy() {
        super.destroyColumnFamilyHandle();
    }

    public List<KafkaOffset> loadOffsets(Collection<TopicPartition> topicPartitions) {
        List<KafkaOffset> kafkaOffsets = new ArrayList<>();
        for (TopicPartition topicPartition : topicPartitions) {
            try {
                byte[] bytes = rocksDB.get(columnFamilyHandle, topicPartition.toString().getBytes());

                if (bytes == null || bytes.length == 0)
                    continue;

                kafkaOffsets.add(new KafkaOffset(topicPartition, CommonConverter.fromBytes(bytes, Long.class)));
            } catch (RocksDBException e) {
                log.error("Reading kafkaOffsets exception:{}", topicPartitions, e);
                throw new RuntimeException();
            }
        }
        return kafkaOffsets;
    }

    public void saveOffsets(List<KafkaOffset> kafkaOffsets) {
        WriteBatch writeBatch = new WriteBatch();
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.setSync(true);
        try {
            for (Map.Entry<String, Long> entry : convertToMap(kafkaOffsets).entrySet()) {
                writeBatch.put(columnFamilyHandle, entry.getKey().getBytes(),
                        CommonConverter.toBytes(entry.getValue()));
            }
            rocksDB.write(writeOptions, writeBatch);
        } catch (RocksDBException e) {
            log.error("Putting to writeBatch exception:{}", kafkaOffsets, e);
            throw new RuntimeException();
        } finally {
            writeBatch.close();
            writeOptions.close();
        }
    }

    public boolean isBeforeCurrentOffsets(List<KafkaOffset> clockKafkaOffsets) {
        List<TopicPartition> clockPartitions = clockKafkaOffsets.stream()
                .map(KafkaOffset::getTopicPartition)
                .collect(Collectors.toList());

        List<KafkaOffset> currentOffsets = loadOffsets(clockPartitions);

        Map<Integer, Long> currentOffsetsLookupMap = currentOffsets.stream()
                .collect(Collectors.toMap(ko -> ko.getTopicPartition().partition(), KafkaOffset::getOffset));

        return clockKafkaOffsets.stream().allMatch(isBefore(currentOffsetsLookupMap));
    }

    private Predicate<KafkaOffset> isBefore(Map<Integer, Long> currentOffsetsLookupMap) {
        return clockKafkaOffset -> {
            Long currentOffset = currentOffsetsLookupMap.get(clockKafkaOffset.getTopicPartition().partition());
            return currentOffset > clockKafkaOffset.getOffset();
        };
    }

    private Map<String, Long> convertToMap(List<KafkaOffset> kafkaOffsets) {
        return kafkaOffsets.stream().collect(
                Collectors.toMap(
                        kafkaOffset -> kafkaOffset.getTopicPartition().toString(),
                        KafkaOffset::getOffset
                )
        );
    }
}
