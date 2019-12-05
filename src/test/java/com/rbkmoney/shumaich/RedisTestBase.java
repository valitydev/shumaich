package com.rbkmoney.shumaich;


import com.rbkmoney.shumaich.config.RedisConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;

@Slf4j
@RunWith(SpringRunner.class)
@ContextConfiguration(initializers = RedisTestBase.Initializer.class, classes = RedisConfiguration.class)
public abstract class RedisTestBase {

    @ClassRule
    public static GenericContainer<?> redis = new GenericContainer<>("redis:5.0.7")
            .withExposedPorts(6379);

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues
                    .of("redis.host=" + redis.getContainerIpAddress(),
                            "redis.port=" + redis.getMappedPort(6379))
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }

}
