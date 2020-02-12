package com.rbkmoney.shumaich.kafka;

import com.rbkmoney.shumaich.IntegrationTestBase;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.service.Handler;
import com.rbkmoney.shumaich.service.OperationLogHandlingService;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.rbkmoney.shumaich.helpers.TestData.OPERATION_LOG_TOPIC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

/**
 * Every test should use different partition, cause Kafka doesn't provide any method to *reliably* clear topics.
 */
@Slf4j
@ContextConfiguration(classes = {SimpleTopicConsumerIntegrationTest.Config.class})
public class SimpleTopicConsumerIntegrationTest extends IntegrationTestBase {

    @Autowired
    Handler<OperationLog> operationLogHandler;

    @Autowired
    KafkaTemplate<String, OperationLog> operationLogKafkaTemplate;

    @Autowired
    TopicConsumptionManager<String, OperationLog> operationLogTopicConsumptionManager;

    @Before
    public void clear() throws InterruptedException {
        operationLogTopicConsumptionManager.shutdownConsumers();
    }

    @After
    public void cleanup() throws IOException {
        folder.delete();
        folder.create();
    }

    @Test
    public void testCreationAndInteraction() throws InterruptedException, ExecutionException {
        int testPartition = 0;
        sendOperationLogToPartition(testPartition);

        Thread.sleep(2000);

        Mockito.verify(operationLogHandler, Mockito.atLeast(1)).handle(any());
        checkOffsets(testPartition, 1L, OPERATION_LOG_TOPIC);

        IntStream.range(0, 10).forEach(ignore -> sendOperationLogToPartition(testPartition));

        Thread.sleep(2000);

        Mockito.verify(operationLogHandler, Mockito.atLeast(2)).handle(any());
        checkOffsets(testPartition, 11L, OPERATION_LOG_TOPIC);
    }

    @Test
    public void offsetsLoadedOnStartup() throws ExecutionException, InterruptedException {
        int testPartition = 1;
        setInitialOffsets(testPartition, 10L, OPERATION_LOG_TOPIC);

        AtomicInteger receivedRecordsSize = new AtomicInteger(0);
        registerReceivedMessages(1, receivedRecordsSize, operationLogHandler);

        //reloading consumers for offset change
        operationLogTopicConsumptionManager.shutdownConsumers();

        //writing data
        for (int i = 0; i < 20; i++) {
            sendOperationLogToPartition(testPartition);
        }

        //waiting consumers to wake up
        Thread.sleep(3000);

        //we skipped 10 messages, assuming to have 10 more in partition 1
        Assert.assertEquals(10, receivedRecordsSize.get());
        checkOffsets(testPartition, 20L, OPERATION_LOG_TOPIC);
    }

    @Test
    public void randomExceptionInMessageProcessing() throws InterruptedException, ExecutionException {
        int testPartition = 2;

        Mockito.doThrow(RuntimeException.class)
                .doThrow(RuntimeException.class)
                .doNothing()
                .when(operationLogHandler).handle(any());

        sendOperationLogToPartition(testPartition);

        Thread.sleep(6000);

        Mockito.verify(operationLogHandler, Mockito.atLeast(3)).handle(any());
        checkOffsets(testPartition, 1L, OPERATION_LOG_TOPIC);
    }

    @Test
    public void handledMessageWithExceptionWhenSavingToRedis() throws ExecutionException, InterruptedException {
        int testPartition = 3;

        Mockito.doThrow(RuntimeException.class)
                .doCallRealMethod()
                .when(kafkaOffsetDao)
                .saveOffsets(any());

        sendOperationLogToPartition(testPartition);

        Thread.sleep(5000);

        Mockito.verify(operationLogHandler, Mockito.atLeast(2)).handle(any());
        checkOffsets(testPartition, 1L, OPERATION_LOG_TOPIC);
    }

    @Configuration
    public static class Config {

        @Bean
        @Primary
        Handler<OperationLog> operationLogHandler() {
            return mock(OperationLogHandlingService.class);
        }

    }
}
