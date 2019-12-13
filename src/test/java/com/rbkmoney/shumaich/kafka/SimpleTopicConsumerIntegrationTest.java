package com.rbkmoney.shumaich.kafka;

import com.rbkmoney.shumaich.IntegrationTestBase;
import com.rbkmoney.shumaich.TestData;
import com.rbkmoney.shumaich.TestUtils;
import com.rbkmoney.shumaich.dao.KafkaOffsetDao;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.RequestLog;
import com.rbkmoney.shumaich.service.Handler;
import com.rbkmoney.shumaich.service.OperationLogHandlingService;
import com.rbkmoney.shumaich.service.RequestLogHandlingService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.rbkmoney.shumaich.TestData.REQUEST_LOG_TOPIC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

/**
 * Every test should use different partition, cause Kafka doesn't provide any method to *reliably* clear topics.
 */
@Slf4j
@ContextConfiguration(classes = {SimpleTopicConsumerIntegrationTest.Config.class})
public class SimpleTopicConsumerIntegrationTest extends IntegrationTestBase {

    @Autowired
    Handler<RequestLog> requestLogHandler;

    @Autowired
    Handler<OperationLog> operationLogHandler;

    @Autowired
    KafkaTemplate<String, RequestLog> requestLogKafkaTemplate;

    @Autowired
    KafkaTemplate<String, OperationLog> operationLogKafkaTemplate;

    @Autowired
    AdminClient kafkaAdminClient;

    @SpyBean
    KafkaOffsetDao kafkaOffsetDao;

    @Autowired
    TopicConsumptionManager<String, RequestLog> requestLogTopicConsumptionManager;

    @Autowired
    RedisTemplate<String, KafkaOffset> kafkaOffsetRedisTemplate;

    @Before
    public void clear() throws InterruptedException {
        TestUtils.deleteOffsets(kafkaOffsetRedisTemplate);
        requestLogTopicConsumptionManager.shutdownConsumers();
    }

    @Test
    public void testCreationAndInteraction() throws InterruptedException, ExecutionException {
        int testPartition = 0;
        sendRequestLogToPartition(testPartition);

        Thread.sleep(2000);

        Mockito.verify(requestLogHandler, Mockito.atLeast(1)).handle(any());
        checkOffsets(testPartition, 1L);

        IntStream.range(0, 10).forEach(ignore -> sendRequestLogToPartition(testPartition));

        Thread.sleep(2000);

        Mockito.verify(requestLogHandler, Mockito.atLeast(2)).handle(any());
        checkOffsets(testPartition, 11L);
    }

    @Test
    public void offsetsLoadedOnStartup() throws ExecutionException, InterruptedException {
        int testPartition = 1;
        setInitialOffsets(testPartition, 10L);

        AtomicInteger receivedRecordsSize = new AtomicInteger(0);
        registerReceivedMessages(1, receivedRecordsSize);

        //reloading consumers for offset change
        requestLogTopicConsumptionManager.shutdownConsumers();

        //writing data
        for (int i = 0; i < 20; i++) {
            sendRequestLogToPartition(testPartition);
        }

        //waiting consumers to wake up
        Thread.sleep(3000);

        //we skipped 10 messages, assuming to have 10 more in partition 1
        Assert.assertEquals(10, receivedRecordsSize.get());
        checkOffsets(testPartition, 20L);
    }

    @Test
    public void randomExceptionInMessageProcessing() throws InterruptedException, ExecutionException {
        int testPartition = 2;

        Mockito.doThrow(RuntimeException.class)
                .doThrow(RuntimeException.class)
                .doNothing()
                .when(requestLogHandler).handle(any());

        sendRequestLogToPartition(testPartition);

        Thread.sleep(6000);

        Mockito.verify(requestLogHandler, Mockito.atLeast(3)).handle(any());
        checkOffsets(testPartition, 1L);
    }

    @Test
    public void handledMessageWithExceptionWhenSavingToRedis() throws ExecutionException, InterruptedException {
        int testPartition = 3;

        Mockito.doThrow(RuntimeException.class)
                .doCallRealMethod()
                .when(kafkaOffsetDao)
                .saveOffsets(any());

        sendRequestLogToPartition(testPartition);

        Thread.sleep(5000);

        Mockito.verify(requestLogHandler, Mockito.atLeast(2)).handle(any());
        checkOffsets(testPartition, 1L);
    }

    private void registerReceivedMessages(int partitionNumber, AtomicInteger receivedRecordsSize) {
        Mockito.doAnswer(invocation -> {
            // As we can't clean records from topics in test - we just parallelize tests by partitions
            ConsumerRecords<String, RequestLog> consumerRecords = (ConsumerRecords<String, RequestLog>) invocation.getArguments()[0];
            Optional<TopicPartition> wantedPartition = consumerRecords.partitions().stream()
                    .filter(topicPartition -> topicPartition.partition() == partitionNumber)
                    .findFirst();
            wantedPartition.ifPresent(topicPartition ->
                    receivedRecordsSize.addAndGet(consumerRecords.records(topicPartition).size())
            );
            return null;
        })
                .when(requestLogHandler)
                .handle(any());
    }

    private void checkOffsets(int partitionNumber, Long expectedOffset) throws ExecutionException, InterruptedException {
        List<TopicPartitionInfo> partitions = getTopicPartitions();

        List<KafkaOffset> kafkaOffsets = kafkaOffsetDao.loadOffsets(partitions.stream()
                .filter(partition -> partition.partition() == partitionNumber)
                .map(topicPartitionInfo -> new TopicPartition(REQUEST_LOG_TOPIC, topicPartitionInfo.partition()))
                .collect(Collectors.toList())
        );

        Assert.assertTrue(kafkaOffsets.toString(), kafkaOffsets.stream().anyMatch(kafkaOffset -> kafkaOffset.getOffset().equals(expectedOffset)));
    }

    private void setInitialOffsets(int partitionNumber, Long initialOffsets) throws InterruptedException, ExecutionException {
        List<TopicPartitionInfo> partitions = getTopicPartitions();

        kafkaOffsetDao.saveOffsets(partitions.stream()
                .filter(partition -> partition.partition() == partitionNumber)
                .map(topicPartitionInfo -> TestData.kafkaOffset(
                        REQUEST_LOG_TOPIC,
                        topicPartitionInfo.partition(),
                        initialOffsets))
                .collect(Collectors.toList()));

    }


    private List<TopicPartitionInfo> getTopicPartitions() throws InterruptedException, ExecutionException {
        return kafkaAdminClient.describeTopics(List.of(REQUEST_LOG_TOPIC))
                .values()
                .get(REQUEST_LOG_TOPIC)
                .get()
                .partitions();
    }

    private void sendRequestLogToPartition(int partitionNumber) {
        RequestLog requestLog = TestData.requestLog();
        requestLogKafkaTemplate.sendDefault(partitionNumber, requestLog.getPlanId(), requestLog);
    }

    @Order
    @Configuration
    public static class Config {

        @Bean
        @Primary
        Handler<RequestLog> requestLogHandler() {
            return mock(RequestLogHandlingService.class);
        }

        @Bean
        @Primary
        Handler<OperationLog> operationLogHandler() {
            return mock(OperationLogHandlingService.class);
        }

    }
}
