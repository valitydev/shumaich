package com.rbkmoney.shumaich.kafka;

import com.rbkmoney.shumaich.dao.KafkaOffsetDao;
import com.rbkmoney.shumaich.handler.Handler;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;

public class TopicConsumptionManager<K, V> {

    private final TopicMetadata topicMetadata;

    private final ExecutorService executorService;
    private List<SimpleTopicConsumer<K, V>> consumers;

    public TopicConsumptionManager(TopicMetadata topicMetadata,
                                   Integer partitionsPerThread,
                                   Map<String, Object> consumerProps,
                                   KafkaOffsetDao kafkaOffsetDao,
                                   Handler<V> handler,
                                   Long pollingTimeout) {
        this.topicMetadata = topicMetadata;
        List<PartitionMetadata> partitionMetadata = topicMetadata.partitionMetadata();
        int consumersAmount = partitionMetadata.size() / partitionsPerThread;
        if (partitionMetadata.size() % partitionsPerThread > 0) {
            consumersAmount++;
        }
        this.executorService = Executors.newFixedThreadPool(consumersAmount);
        for (int i = 0; i < consumersAmount; i++) {
            executorService.submit(
                    new SimpleTopicConsumer<>(
                            new KafkaConsumer<K, V>(consumerProps),
                            calculateAssignedPartitions(topicMetadata, partitionsPerThread, partitionMetadata, i),
                            kafkaOffsetDao,
                            handler,
                            pollingTimeout
                    )
            );
        }
    }

    private List<TopicPartition> calculateAssignedPartitions(TopicMetadata topicMetadata, Integer partitionsPerThread, List<PartitionMetadata> partitionMetadata, int i) {
        return partitionMetadata.subList(i * partitionsPerThread, (i + 1) * partitionsPerThread)
                .stream()
                .map(partitionMeta -> new TopicPartition(
                        topicMetadata.topic(),
                        partitionMeta.partition()))
                .collect(Collectors.toList());
    }
}
