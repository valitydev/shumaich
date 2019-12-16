package com.rbkmoney.shumaich.service;

import com.rbkmoney.shumaich.IntegrationTestBase;
import com.rbkmoney.shumaich.TestUtils;
import com.rbkmoney.shumaich.dao.KafkaOffsetDao;
import com.rbkmoney.shumaich.domain.KafkaOffset;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.RequestLog;
import com.rbkmoney.shumaich.kafka.SimpleTopicConsumerIntegrationTest;
import com.rbkmoney.shumaich.kafka.TopicConsumptionManager;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;

/**
 * Every test should use different partition, cause Kafka doesn't provide any method to *reliably* clear topics.
 */
@Slf4j
@ContextConfiguration(classes = {SimpleTopicConsumerIntegrationTest.Config.class})
public class RequestLogHandlingServiceIntegrationTest extends IntegrationTestBase {

    @SpyBean
    KafkaTemplate<Long, OperationLog> operationLogKafkaTemplate;

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
    public void successEventPropagation() {

    }

}
