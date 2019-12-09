package com.rbkmoney.shumaich.kafka;

import com.rbkmoney.shumaich.dao.KafkaOffsetDao;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.service.Handler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class SimpleTopicConsumer<K, V> implements Runnable {

    private AtomicBoolean alive = new AtomicBoolean(true);
    private KafkaConsumer<K, V> consumer;

    private final Map<String, Object> consumerProps;
    private final List<TopicPartition> assignedPartitions;
    private final KafkaOffsetDao kafkaOffsetDao;
    private final Handler<V> handler;
    private final Long pollingTimeout;

    public static <K, V> SimpleTopicConsumer<K, V> of(SimpleTopicConsumer<K, V> otherConsumer) {
        return new SimpleTopicConsumer<>(
                otherConsumer.consumerProps,
                otherConsumer.assignedPartitions,
                otherConsumer.kafkaOffsetDao,
                otherConsumer.handler,
                otherConsumer.pollingTimeout
        );
    }

    public boolean isAlive() {
        return alive.get();
    }

    @Override
    public void run() {
        try {
            initConsumer();

            while (alive.get()) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(pollingTimeout));
                if (records.count() > 0) {
                    handler.handle(records);
                    saveOffsets(records);
                }
            }

        } catch (WakeupException e) {
            // Ignore exception if closing
            if (alive.get()) {
                log.error("External wakeup call occurred", e);
            }
        } catch (Exception e) {
            alive.set(false);
            log.error("Error during Kafka polling", e);
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        log.info("Closing consumer for topic and partitions: {}", assignedPartitions);
        alive.set(false);
        consumer.wakeup();
    }

    private void initConsumer() {
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.assign(assignedPartitions);
        List<KafkaOffset> kafkaOffsets = kafkaOffsetDao.loadOffsets(assignedPartitions);
        kafkaOffsets.forEach(kafkaOffset -> consumer.seek(kafkaOffset.getTopicPartition(), kafkaOffset.getOffset()));
    }

    private void saveOffsets(ConsumerRecords<K, V> records) {

        List<KafkaOffset> offsets = records.partitions().stream()
                .map(topicPartition -> getLatestKafkaOffset(records, topicPartition))
                .filter(Predicate.not(Objects::isNull))
                .collect(Collectors.toList());

        kafkaOffsetDao.saveOffsets(offsets);
    }

    private KafkaOffset getLatestKafkaOffset(ConsumerRecords<K, V> records, TopicPartition topicPartition) {
        List<ConsumerRecord<K, V>> recordsForPartition = records.records(topicPartition);
        if (recordsForPartition.isEmpty()) return null;

        long lastOffset = recordsForPartition.get(recordsForPartition.size() - 1).offset();
        return new KafkaOffset(topicPartition, lastOffset + 1); // +1 to not repeat read of last message
    }

}
