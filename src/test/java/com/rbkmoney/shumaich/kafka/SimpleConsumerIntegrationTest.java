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
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Assert;
import org.junit.Before;
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
@ContextConfiguration(classes = {SimpleConsumerIntegrationTest.Config.class})
public class SimpleConsumerIntegrationTest extends IntegrationTestBase {

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

    @Before
    public void resetOffsets() {
        Mockito.reset(requestLogHandler);
        TestUtils.deleteOffsets(kafkaOffsetRedisTemplate);
    }

    @Test
    public void testCreationAndInteraction() throws InterruptedException {
        requestLogKafkaTemplate.sendDefault(TestData.requestLog());

        Mockito.verify(requestLogHandler, Mockito.timeout(2000).times(1)).handle(any());
    }

    @Test
    public void offsetsLoadedOnStartup() throws ExecutionException, InterruptedException {
        setInitialOffsets(10L);

        AtomicInteger receivedRecordsSize = new AtomicInteger(0);
        registerReceivedMessages(receivedRecordsSize, "offsetsLoadedOnStartup");

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
    }

    private void registerReceivedMessages(AtomicInteger receivedRecordsSize, String methodName) {
        Mockito.doAnswer(invocation -> {
            log.error("{}: {}", methodName, receivedRecordsSize.get());
            // using compareAndSet as we have multiple consumers
            receivedRecordsSize.compareAndSet(0,
                    ((ConsumerRecords<String, RequestLog>) invocation.getArguments()[0]).count());
            return null;
        })
                .when(requestLogHandler)
                .handle(any());
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
