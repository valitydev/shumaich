package com.rbkmoney.shumaich.service;

import com.rbkmoney.shumaich.converter.CommonConverter;
import com.rbkmoney.shumaich.dao.KafkaOffsetDao;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.exception.DaoException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaOffsetService {

    private final KafkaOffsetDao kafkaOffsetDao;

    public List<KafkaOffset> loadOffsets(Collection<TopicPartition> topicPartitions) {
        List<KafkaOffset> kafkaOffsets = new ArrayList<>();
        for (TopicPartition topicPartition : topicPartitions) {
            Long offset = kafkaOffsetDao.get(topicPartition.toString());

            if (offset == null)
                continue;

            kafkaOffsets.add(new KafkaOffset(topicPartition, offset));
        }
        return kafkaOffsets;
    }

    public void saveOffsets(List<KafkaOffset> kafkaOffsets) {
        try (WriteBatch writeBatch = new WriteBatch();
             WriteOptions writeOptions = new WriteOptions().setSync(true)) {
            prepareBatch(kafkaOffsets, writeBatch);
            kafkaOffsetDao.putBatch(writeOptions, writeBatch);
        } catch (RocksDBException e) {
            log.error("Putting kafkaOffset to writeBatch exception:{}", kafkaOffsets, e);
            throw new DaoException("Putting kafkaOffset to writeBatch exception: " + kafkaOffsets, e);
        }
    }

    private void prepareBatch(List<KafkaOffset> kafkaOffsets, WriteBatch writeBatch) throws RocksDBException {
        for (Map.Entry<String, Long> entry : convertToMap(kafkaOffsets).entrySet()) {
            writeBatch.put(kafkaOffsetDao.getColumnFamilyHandle(), entry.getKey().getBytes(),
                    CommonConverter.toBytes(entry.getValue()));
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
