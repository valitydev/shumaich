package com.rbkmoney.shumaich.dao;

import com.rbkmoney.shumaich.converter.CommonConverter;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.exception.DaoException;
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

    public Long get(String topicPartition) {
        try {
            return CommonConverter.fromBytes(rocksDB.get(columnFamilyHandle, topicPartition.getBytes()), Long.class);
        } catch (RocksDBException e) {
            log.error("Can't get kafkaOffset topicPartition with ID: {}", topicPartition, e);
            throw new DaoException("Can't get kafkaOffset topicPartition with ID: " + topicPartition, e);
        }
    }

    public void putBatch(WriteOptions writeOptions, WriteBatch writeBatch) throws RocksDBException {
            rocksDB.write(writeOptions, writeBatch);
    }
}
