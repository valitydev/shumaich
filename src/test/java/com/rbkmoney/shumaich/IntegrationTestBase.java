package com.rbkmoney.shumaich;


import lombok.extern.slf4j.Slf4j;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;

import static com.rbkmoney.shumaich.TestData.OPERATION_LOG_TOPIC;
import static com.rbkmoney.shumaich.TestData.REQUEST_LOG_TOPIC;

@Slf4j
@RunWith(SpringRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(classes = ShumaichApplication.class)
@ContextConfiguration(initializers = IntegrationTestBase.Initializer.class)
public abstract class IntegrationTestBase {

    @ClassRule
    public static EmbeddedKafkaRule kafka = new EmbeddedKafkaRule(1, true, 5,
            REQUEST_LOG_TOPIC, OPERATION_LOG_TOPIC);

    @ClassRule
    public static GenericContainer<?> redis = new GenericContainer<>("redis:5.0.7")
            .withExposedPorts(6379);

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues
                    .of("kafka.bootstrap.servers=" + kafka.getEmbeddedKafka().getBrokersAsString(),
                            "redis.host=" + redis.getContainerIpAddress(),
                            "redis.port=" + redis.getMappedPort(6379))
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }

}
