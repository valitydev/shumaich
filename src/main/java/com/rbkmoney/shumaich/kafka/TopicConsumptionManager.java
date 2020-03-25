package com.rbkmoney.shumaich.kafka;

import com.rbkmoney.shumaich.service.Handler;
import com.rbkmoney.shumaich.service.KafkaOffsetService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
public class TopicConsumptionManager<K, V> {

    private final ExecutorService executorService;
    private final List<SimpleTopicConsumer<K, V>> consumers = new ArrayList<>();

    public TopicConsumptionManager(TopicDescription topicDescription,
                                   Integer partitionsPerThread,
                                   Map<String, Object> consumerProps,
                                   KafkaOffsetService kafkaOffsetService,
                                   Handler<V> handler,
                                   Long pollingTimeout) {
        List<TopicPartitionInfo> topicPartitions = topicDescription.partitions();
        int consumersAmount = (int) Math.ceil(topicPartitions.size() / (double) partitionsPerThread);
        this.executorService = Executors.newFixedThreadPool(consumersAmount);

        for (int i = 0; i < consumersAmount; i++) {
            consumers.add(
                    new SimpleTopicConsumer<>(
                            consumerProps,
                            calculateAssignedPartitions(partitionsPerThread, topicDescription, i),
                            kafkaOffsetService,
                            handler,
                            pollingTimeout
                    )
            );
        }
    }

    @PostConstruct
    public void submitConsumers() {
        consumers.forEach(executorService::submit);
    }

    @Scheduled(fixedRateString = "${kafka.topics.consumer-monitor-rate}")
    public void monitorConsumers() {
        List<SimpleTopicConsumer<K, V>> deadConsumers = consumers.stream()
                .filter(Predicate.not(SimpleTopicConsumer::isAlive))
                .collect(Collectors.toList());

        consumers.removeAll(deadConsumers);
        consumers.addAll(restartDeadConsumers(deadConsumers));
    }

    @PreDestroy
    public void shutdownConsumers() {
        consumers.forEach(SimpleTopicConsumer::shutdown);
    }

    private List<SimpleTopicConsumer<K, V>> restartDeadConsumers(List<SimpleTopicConsumer<K, V>> deadConsumers) {
        return deadConsumers.stream()
                .map(SimpleTopicConsumer::of)
                .peek(executorService::submit)
                .collect(Collectors.toList());
    }

    private List<TopicPartition> calculateAssignedPartitions(Integer partitionsPerThread,
                                                             TopicDescription topicDescription,
                                                             int i) {
        int fromIndex = i * partitionsPerThread;
        int toIndex = fromIndex + partitionsPerThread > topicDescription.partitions().size()
                ? fromIndex + (topicDescription.partitions().size() % partitionsPerThread)
                : fromIndex + partitionsPerThread;

        return topicDescription.partitions().subList(fromIndex, toIndex)
                .stream()
                .map(partitionInfo -> new TopicPartition(
                        topicDescription.name(),
                        partitionInfo.partition()))
                .collect(Collectors.toList());
    }

}
