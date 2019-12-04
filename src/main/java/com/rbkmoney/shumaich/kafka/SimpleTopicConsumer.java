package com.rbkmoney.shumaich.kafka;

import com.rbkmoney.shumaich.dao.KafkaOffsetDao;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.handler.Handler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class SimpleTopicConsumer<K, V> implements Runnable {

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer<K, V> consumer;
    private final List<TopicPartition> assignedPartitions;
    private final KafkaOffsetDao kafkaOffsetDao;
    private final Handler<V> handler;
    private final Long pollingTimeout;

    @Override
    public void run() {
        try {
            initConsumer();

            while (!closed.get()) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(pollingTimeout));
                handler.handle(records);
                saveOffsets(records);
            }

        } catch (WakeupException e) {
            // Ignore exception if closing
            //todo do we need to save offsets here?
            if (!closed.get()) {
                throw e;
            }
        } catch (Exception e) {
            //todo do we need to save offsets here?
            log.error("Some error during Kafka polling", e);
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        closed.set(true);
    }

    private void initConsumer() {
        consumer.assign(assignedPartitions);
        List<KafkaOffset> kafkaOffsets = kafkaOffsetDao.loadOffsets(assignedPartitions);
        kafkaOffsets.forEach(kafkaOffset -> consumer.seek(kafkaOffset.getTopicPartition(), kafkaOffset.getOffset()));
    }

    private void saveOffsets(ConsumerRecords<K, V> records) {

        List<KafkaOffset> offsets = records.partitions().stream().map(topicPartition -> {
            List<ConsumerRecord<K, V>> recordsForPartition = records.records(topicPartition);
            if (recordsForPartition.isEmpty()) return null;

            long lastOffset = recordsForPartition.get(recordsForPartition.size() - 1).offset();
            return new KafkaOffset(topicPartition, lastOffset);
        }).collect(Collectors.toList());

        kafkaOffsetDao.saveOffsets(offsets);
    }

}
