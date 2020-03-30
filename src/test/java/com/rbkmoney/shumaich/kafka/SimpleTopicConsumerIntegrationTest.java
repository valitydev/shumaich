package com.rbkmoney.shumaich.kafka;

import com.rbkmoney.shumaich.IntegrationTestBase;
import com.rbkmoney.shumaich.dao.KafkaOffsetDao;
import com.rbkmoney.shumaich.service.Handler;
import com.rbkmoney.shumaich.service.KafkaOffsetService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joor.Reflect;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.rocksdb.RocksDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.rbkmoney.shumaich.helpers.TestData.TEST_TOPIC;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

/**
 * Every test should use different partition, cause Kafka doesn't provide any method to *reliably* clear topics.
 */
@Slf4j
@ContextConfiguration(classes = {SimpleTopicConsumerIntegrationTest.Config.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class SimpleTopicConsumerIntegrationTest extends IntegrationTestBase {

    @Autowired
    Handler<String> testLogHandler;

    @Autowired
    KafkaTemplate<String, String> testLogKafkaTemplate;

    @Autowired
    TopicConsumptionManager<String, String> testLogTopicConsumptionManager;

    @Autowired
    RocksDB rocksDB;

    @Autowired
    KafkaOffsetDao kafkaOffsetDao;

    @Autowired
    KafkaOffsetService kafkaOffsetService;

    @Test
    public void testCreationAndInteraction() throws InterruptedException, ExecutionException {
        int testPartition = 0;
        sendTestLogToPartition(testPartition);

        await().atMost(10, SECONDS).untilAsserted(() -> {
            Mockito.verify(testLogHandler, Mockito.atLeast(1)).handle(any());
            checkOffsets(testPartition, 1L, TEST_TOPIC);
        });


        IntStream.range(0, 10).forEach(ignore -> sendTestLogToPartition(testPartition));

        await().atMost(10, SECONDS).untilAsserted(() -> {
            Mockito.verify(testLogHandler, Mockito.atLeast(2)).handle(any());
            checkOffsets(testPartition, 11L, TEST_TOPIC);

        });
    }

    @Test
    public void offsetsLoadedOnStartup() throws ExecutionException, InterruptedException {
        int testPartition = 1;
        setInitialOffsets(testPartition, 10L, TEST_TOPIC);

        AtomicInteger receivedRecordsSize = new AtomicInteger(0);
        registerReceivedMessages(1, receivedRecordsSize, testLogHandler);

        //reloading consumers for offset change
        testLogTopicConsumptionManager.shutdownConsumersGracefully();
        Reflect.on(testLogTopicConsumptionManager).set("destroying", new AtomicBoolean(false));

        //writing data
        for (int i = 0; i < 20; i++) {
            sendTestLogToPartition(testPartition);
        }

        //waiting consumers to wake up
        await().atMost(10, SECONDS).untilAsserted(() -> {
            //we skipped 10 messages, assuming to have 10 more in partition 1
            Assert.assertEquals(10, receivedRecordsSize.get());
            checkOffsets(testPartition, 20L, TEST_TOPIC);
        });
    }

    @Test
    public void randomExceptionInMessageProcessing() throws InterruptedException, ExecutionException {
        int testPartition = 2;

        Mockito.doThrow(RuntimeException.class)
                .doThrow(RuntimeException.class)
                .doNothing()
                .when(testLogHandler).handle(any());

        sendTestLogToPartition(testPartition);

        await().atMost(10, SECONDS).untilAsserted(() -> {
            Mockito.verify(testLogHandler, Mockito.atLeast(3)).handle(any());
            checkOffsets(testPartition, 1L, TEST_TOPIC);
        });
    }

    @Test
    public void handledMessageWithExceptionWhenSavingToRedis() throws ExecutionException, InterruptedException {
        int testPartition = 3;

        Mockito.doThrow(RuntimeException.class)
                .doCallRealMethod()
                .when(kafkaOffsetService)
                .saveOffsets(any());

        sendTestLogToPartition(testPartition);

        await().atMost(10, SECONDS).untilAsserted(() -> {
            Mockito.verify(testLogHandler, Mockito.atLeast(2)).handle(any());
            checkOffsets(testPartition, 1L, TEST_TOPIC);
        });
    }

    private void sendTestLogToPartition(int testPartition) {
        testLogKafkaTemplate.sendDefault(testPartition, "test", "test");
    }

    @Configuration
    public static class Config {

        @Value("${kafka.topics.partitions-per-thread}")
        private Integer partitionsPerThread;

        @Value("${kafka.topics.polling-timeout}")
        private Long pollingTimeout;

        @Bean
        @Primary
        Handler<String> testLogHandler() {
            return mock(Handler.class);
        }

        private static final String EARLIEST = "earliest";

        @Bean
        @DependsOn(value = "rocksDB")
        public KafkaTemplate<String, String> testLogKafkaTemplate() {
            Map<String, Object> configs = KafkaTestUtils.producerProps(kafka.getEmbeddedKafka());
            configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(
                    new DefaultKafkaProducerFactory<>(configs), true);
            kafkaTemplate.setDefaultTopic(TEST_TOPIC);
            return kafkaTemplate;
        }

        @Bean
        @DependsOn(value = "rocksDB")
        public TopicConsumptionManager<String, String> testLogTopicConsumptionManager(AdminClient kafkaAdminClient,
                                                                                      KafkaOffsetService kafkaOffsetService,
                                                                                      Handler<String> handler) throws ExecutionException, InterruptedException {
            Map<String, Object> consumerProps = new HashMap<>();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getEmbeddedKafka().getBrokersAsString());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

            TopicDescription topicDescription = kafkaAdminClient
                    .describeTopics(List.of(TEST_TOPIC))
                    .values()
                    .get(TEST_TOPIC)
                    .get();

            return new TopicConsumptionManager<>(topicDescription,
                    partitionsPerThread,
                    consumerProps,
                    kafkaOffsetService,
                    handler,
                    pollingTimeout
            );
        }

    }
}
