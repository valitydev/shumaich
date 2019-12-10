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
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.rbkmoney.shumaich.TestData.REQUEST_LOG_TOPIC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

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

    @Autowired
    KafkaOffsetDao kafkaOffsetDao;

    @Autowired
    TopicConsumptionManager<String, RequestLog> requestLogTopicConsumptionManager;

    @Autowired
    RedisTemplate<String, KafkaOffset> kafkaOffsetRedisTemplate;

    @After
    public void clear() throws InterruptedException {
        Mockito.reset(requestLogHandler);
        TestUtils.deleteOffsets(kafkaOffsetRedisTemplate);
        clearTopic(REQUEST_LOG_TOPIC, 100);
    }

    @Test
    public void testCreationAndInteraction() throws InterruptedException, ExecutionException {
        requestLogKafkaTemplate.sendDefault(TestData.requestLog());

        Mockito.verify(requestLogHandler, Mockito.timeout(2000).times(1)).handle(any());
        checkOffsets(1L);
    }

    @Test
    public void offsetsLoadedOnStartup() throws ExecutionException, InterruptedException {
        setInitialOffsets(10L);

        AtomicInteger receivedRecordsSize = new AtomicInteger(0);
        registerReceivedMessages(receivedRecordsSize);

        //reloading consumers for offset change
        requestLogTopicConsumptionManager.shutdownConsumers();

        //writing data
        for (int i = 0; i < 20; i++) {
            RequestLog requestLog = TestData.requestLog();
            requestLogKafkaTemplate.sendDefault(0, requestLog.getPlanId(), requestLog);
        }

        //waiting consumers to wake up
        Thread.sleep(3000);

        //we skipped 10 messages, assuming to have 10 more in partition 0
        Assert.assertEquals(10, receivedRecordsSize.get());
        checkOffsets(20L);
    }

    @Test
    public void randomExceptionInMessageProcessing() throws InterruptedException, ExecutionException {
        Mockito.doThrow(RuntimeException.class)
                .doThrow(RuntimeException.class)
                .doNothing()
                .when(requestLogHandler).handle(any());
        requestLogKafkaTemplate.sendDefault(TestData.requestLog());

        Mockito.verify(requestLogHandler, Mockito.timeout(6000).times(3)).handle(any());
        checkOffsets(1L);
    }



    private void registerReceivedMessages(AtomicInteger receivedRecordsSize) {
        Mockito.doAnswer(invocation -> {
            // using addAndGet as we have multiple consumers
            receivedRecordsSize.addAndGet(((ConsumerRecords<String, RequestLog>) invocation.getArguments()[0]).count());
            return null;
        })
                .when(requestLogHandler)
                .handle(any());
    }

    private void checkOffsets(Long expectedOffset) throws ExecutionException, InterruptedException {
        List<TopicPartitionInfo> partitions = kafkaAdminClient.describeTopics(List.of(REQUEST_LOG_TOPIC))
                .values()
                .get(REQUEST_LOG_TOPIC)
                .get()
                .partitions();

        List<KafkaOffset> kafkaOffsets = kafkaOffsetDao.loadOffsets(partitions.stream()
                .map(topicPartitionInfo -> new TopicPartition(REQUEST_LOG_TOPIC, topicPartitionInfo.partition()))
                .collect(Collectors.toList())
        );

        Assert.assertTrue(kafkaOffsets.toString(), kafkaOffsets.stream().anyMatch(kafkaOffset -> kafkaOffset.getOffset().equals(expectedOffset)));
    }


    private void setInitialOffsets(Long initialOffsets) throws InterruptedException, ExecutionException {
        List<TopicPartitionInfo> partitions = kafkaAdminClient.describeTopics(List.of(REQUEST_LOG_TOPIC))
                .values()
                .get(REQUEST_LOG_TOPIC)
                .get()
                .partitions();

        kafkaOffsetDao.saveOffsets(partitions.stream()
                .map(topicPartitionInfo -> TestData.kafkaOffset(
                        REQUEST_LOG_TOPIC,
                        topicPartitionInfo.partition(),
                        initialOffsets))
                .collect(Collectors.toList()));

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
