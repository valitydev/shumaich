package com.rbkmoney.shumaich.kafka;

import com.rbkmoney.shumaich.IntegrationTestBase;
import com.rbkmoney.shumaich.TestData;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.RequestLog;
import com.rbkmoney.shumaich.service.Handler;
import com.rbkmoney.shumaich.service.OperationLogHandlingService;
import com.rbkmoney.shumaich.service.RequestLogHandlingService;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;

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

    @Test
    public void testCreationAndInteraction() throws InterruptedException {
        RequestLog requestLog = TestData.requestLog();
        requestLogKafkaTemplate.sendDefault(requestLog.getPlanId(), requestLog);

        Mockito.verify(requestLogHandler, Mockito.timeout(2000).times(1)).handle(any());
    }

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
