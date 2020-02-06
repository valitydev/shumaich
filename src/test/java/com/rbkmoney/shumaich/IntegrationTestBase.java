package com.rbkmoney.shumaich;


import com.rbkmoney.shumaich.dao.KafkaOffsetDao;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.domain.RequestLog;
import com.rbkmoney.shumaich.helpers.TestData;
import com.rbkmoney.shumaich.service.Handler;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.rbkmoney.shumaich.helpers.TestData.OPERATION_LOG_TOPIC;
import static com.rbkmoney.shumaich.helpers.TestData.REQUEST_LOG_TOPIC;
import static org.mockito.ArgumentMatchers.any;

@Slf4j
@RunWith(SpringRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@SpringBootTest(classes = ShumaichApplication.class)
@ContextConfiguration(initializers = IntegrationTestBase.Initializer.class)
public abstract class IntegrationTestBase {

    @Autowired
    protected AdminClient kafkaAdminClient;

    @Autowired
    protected KafkaTemplate<String, RequestLog> requestLogKafkaTemplate;

    @SpyBean
    protected KafkaOffsetDao kafkaOffsetDao;

    @ClassRule
    public static EmbeddedKafkaRule kafka = new EmbeddedKafkaRule(1, true, 10,
            REQUEST_LOG_TOPIC, OPERATION_LOG_TOPIC);

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @SneakyThrows
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues
                    .of("kafka.bootstrap.servers=" + kafka.getEmbeddedKafka().getBrokersAsString(),
                            "rocksdb.name=test" ,
                            "rocksdb.dir=" + folder.newFolder())
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }

    protected List<TopicPartitionInfo> getTopicPartitions(String topicName) throws InterruptedException, ExecutionException {
        return kafkaAdminClient.describeTopics(List.of(topicName))
                .values()
                .get(topicName)
                .get()
                .partitions();
    }

    protected void setInitialOffsets(int partitionNumber, Long initialOffsets, String topicName) throws InterruptedException, ExecutionException {
        List<TopicPartitionInfo> partitions = getTopicPartitions(topicName);

        kafkaOffsetDao.saveOffsets(partitions.stream()
                .filter(partition -> partition.partition() == partitionNumber)
                .map(topicPartitionInfo -> TestData.kafkaOffset(
                        topicName,
                        topicPartitionInfo.partition(),
                        initialOffsets))
                .collect(Collectors.toList()));

    }

    protected void checkOffsets(int partitionNumber, Long expectedOffset, String topicName) throws ExecutionException, InterruptedException {
        List<TopicPartitionInfo> partitions = getTopicPartitions(topicName);

        List<KafkaOffset> kafkaOffsets = kafkaOffsetDao.loadOffsets(partitions.stream()
                .filter(partition -> partition.partition() == partitionNumber)
                .map(topicPartitionInfo -> new TopicPartition(topicName, topicPartitionInfo.partition()))
                .collect(Collectors.toList())
        );

        Assert.assertTrue(kafkaOffsets.toString(), kafkaOffsets.stream().anyMatch(kafkaOffset -> kafkaOffset.getOffset().equals(expectedOffset)));
    }

    protected <T> void registerReceivedMessages(int partitionNumber, AtomicInteger receivedRecordsSize, Handler<T> handler) {
        Mockito.doAnswer(invocation -> {
            // As we can't clean records from topics in test - we just parallelize tests by partitions
            ConsumerRecords<?, T> consumerRecords = (ConsumerRecords<?, T>) invocation.getArguments()[0];
            Optional<TopicPartition> wantedPartition = consumerRecords.partitions().stream()
                    .filter(topicPartition -> topicPartition.partition() == partitionNumber)
                    .findFirst();
            wantedPartition.ifPresent(topicPartition ->
                    receivedRecordsSize.addAndGet(consumerRecords.records(topicPartition).size())
            );
            return null;
        })
                .when(handler)
                .handle(any());
    }

    protected void sendRequestLogToPartition(int partitionNumber) {
        RequestLog requestLog = TestData.requestLog();
        requestLogKafkaTemplate.sendDefault(partitionNumber, requestLog.getPlanId(), requestLog);
    }

    protected void sendRequestLogToPartition(RequestLog requestLog) {
        requestLogKafkaTemplate.sendDefault(requestLog.getPlanId(), requestLog);
    }

}
