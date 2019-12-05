package com.rbkmoney.shumaich;


import lombok.extern.slf4j.Slf4j;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;

import static com.rbkmoney.shumaich.TestData.OPERATION_LOG_TOPIC;
import static com.rbkmoney.shumaich.TestData.REQUEST_LOG_TOPIC;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = ShumaichApplication.class)
@ContextConfiguration(initializers = IntegrationTestBase.Initializer.class)
public abstract class IntegrationTestBase {

    public static final String CONFLUENT_PLATFORM_VERSION = "5.0.1";

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(CONFLUENT_PLATFORM_VERSION).withEmbeddedZookeeper();

    static {
        createKafkaTopics();
    }

    @ClassRule
    public static GenericContainer<?> redis = new GenericContainer<>("redis:5.0.7")
            .withExposedPorts(6379);

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues
                    .of("kafka.bootstrap.servers=" + kafka.getBootstrapServers(),
                            "redis.host=" + redis.getContainerIpAddress(),
                            "redis.port=" + redis.getMappedPort(6379))
                    .applyTo(configurableApplicationContext.getEnvironment());
        }

    }

    private static void createKafkaTopics() {
        try {
            kafka.start();
            kafka.execInContainer("/bin/sh", "-c", String.format(
                    "/usr/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1" +
                            " --partitions 10 --topic %s", REQUEST_LOG_TOPIC));
            kafka.execInContainer("/bin/sh", "-c", String.format(
                    "/usr/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1" +
                            " --partitions 10 --topic %s", OPERATION_LOG_TOPIC));
        } catch (Exception e) {
            System.err.println("kek");
        }
    }

}
