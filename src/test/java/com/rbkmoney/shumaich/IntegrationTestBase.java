package com.rbkmoney.shumaich;


import com.rbkmoney.shumaich.kafka.serde.OperationLogDeserializer;
import com.rbkmoney.shumaich.kafka.serde.RequestLogDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.ClassRule;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@ContextConfiguration(initializers = IntegrationTestBase.Initializer.class)
public class IntegrationTestBase {

    public static final String CONFLUENT_PLATFORM_VERSION = "5.0.1";

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(CONFLUENT_PLATFORM_VERSION).withEmbeddedZookeeper();

    @ClassRule
    public static GenericContainer<?> redis = new GenericContainer<>("redis").withExposedPorts(6379);

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues
                    .of("kafka.bootstrap.servers=" + kafka.getBootstrapServers())
                    .applyTo(configurableApplicationContext.getEnvironment());
            initTopic("request_log", RequestLogDeserializer.class);
            initTopic("operation_log", OperationLogDeserializer.class);
        }

        private void initTopic(String topicName, Class<?> clazz) {
            Consumer<?, ?> consumer = createConsumer(clazz);
            try {
                consumer.subscribe(Collections.singletonList(topicName));
                consumer.poll(Duration.ofMillis(100L));
            } catch (Exception e) {
                log.error("KafkaAbstractTest initialize e: ", e);
            }
            consumer.close();
        }
    }

    static <T> Consumer<String, T> createConsumer(Class<?> clazz) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, clazz);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

}
