package com.rbkmoney.shumaich.service;

import com.rbkmoney.shumaich.IntegrationTestBase;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import com.rbkmoney.shumaich.helpers.IdempotentTestHandler;
import com.rbkmoney.shumaich.helpers.TestData;
import com.rbkmoney.shumaich.kafka.TopicConsumptionManager;
import lombok.extern.slf4j.Slf4j;
import org.junit.*;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@Slf4j
@ContextConfiguration(classes = {OperationLogHandlerServiceIntegrationTest.Config.class})
public class OperationLogHandlerServiceIntegrationTest extends IntegrationTestBase {

    @SpyBean
    KafkaTemplate<String, OperationLog> operationLogKafkaTemplate;

    @Autowired
    IdempotentTestHandler handler;

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
    public void successEventPropagation() throws InterruptedException, ExecutionException {
        String testPlanId = "plan1";
        PostingPlanOperation plan = TestData.postingPlanOperation(testPlanId);
        sendPlanToPartition(plan);

        Thread.sleep(6000);

        int totalPostings = plan.getPostingBatches().stream()
                .mapToInt(postingBatch -> postingBatch.getPostings().size()).sum();

        Assert.assertEquals(totalPostings * 2,
                handler.countReceivedRecords(testPlanId).intValue());
    }


    @Test
    @Ignore // cейчас flush() нет, возможно верну и тест верну
    public void kafkaTemplateFlushErrorRetried() throws InterruptedException {
        Mockito.doThrow(RuntimeException.class)
                .doCallRealMethod()
                .when(operationLogKafkaTemplate).flush();

        String testPlanId = "plan3";
        PostingPlanOperation plan = TestData.postingPlanOperation(testPlanId);
        sendPlanToPartition(plan);

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
