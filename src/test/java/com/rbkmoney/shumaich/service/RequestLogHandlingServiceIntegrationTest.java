package com.rbkmoney.shumaich.service;

import com.rbkmoney.shumaich.IntegrationTestBase;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.RequestLog;
import com.rbkmoney.shumaich.helpers.IdempotentTestHandler;
import com.rbkmoney.shumaich.helpers.TestData;
import com.rbkmoney.shumaich.kafka.TopicConsumptionManager;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;

import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.any;

@Slf4j
@ContextConfiguration(classes = {RequestLogHandlingServiceIntegrationTest.Config.class})
public class RequestLogHandlingServiceIntegrationTest extends IntegrationTestBase {

    @SpyBean
    KafkaTemplate<Long, OperationLog> operationLogKafkaTemplate;

    @Autowired
    IdempotentTestHandler handler;

    @Autowired
    TopicConsumptionManager<String, RequestLog> requestLogTopicConsumptionManager;


    @Before
    public void clear() throws InterruptedException {
        requestLogTopicConsumptionManager.shutdownConsumers();
    }

    @Test
    public void successEventPropagation() throws InterruptedException, ExecutionException {
        String testPlanId = "plan1";
        RequestLog plan = TestData.requestLog(testPlanId);
        sendRequestLogToPartition(plan);

        Thread.sleep(6000);

        int totalPostings = plan.getPostingBatches().stream()
                .mapToInt(postingBatch -> postingBatch.getPostings().size()).sum();

        Assert.assertEquals(totalPostings * 2,
                handler.countReceivedRecords(testPlanId).intValue());
    }

    @Test
    public void kafkaTemplateSendingErrorRetried() throws InterruptedException {
        Mockito.when(operationLogKafkaTemplate.sendDefault(any(), any()))
                .thenThrow(RuntimeException.class)
                .thenCallRealMethod();

        String testPlanId = "plan2";
        RequestLog plan = TestData.requestLog(testPlanId);
        sendRequestLogToPartition(plan);

        Thread.sleep(6000);

        int totalPostings = plan.getPostingBatches().stream()
                .mapToInt(postingBatch -> postingBatch.getPostings().size()).sum();

        Assert.assertEquals(totalPostings * 2,
                handler.countReceivedRecords(testPlanId).intValue());
    }

    @Test
    public void kafkaTemplateFlushErrorRetried() throws InterruptedException {
        Mockito.doThrow(RuntimeException.class)
                .doCallRealMethod()
                .when(operationLogKafkaTemplate).flush();

        String testPlanId = "plan3";
        RequestLog plan = TestData.requestLog(testPlanId);
        sendRequestLogToPartition(plan);

        Thread.sleep(6000);

        int totalPostings = plan.getPostingBatches().stream()
                .mapToInt(postingBatch -> postingBatch.getPostings().size()).sum();

        Assert.assertEquals(totalPostings * 2,
                handler.countReceivedRecords(testPlanId).intValue());
    }

    @Configuration
    public static class Config {

        @Bean
        @Primary
        Handler<OperationLog> operationLogHandler() {
            return new IdempotentTestHandler();
        }

    }
}
