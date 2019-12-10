package com.rbkmoney.shumaich;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.rbkmoney.shumaich.TestData.OPERATION_LOG_TOPIC;
import static com.rbkmoney.shumaich.TestData.REQUEST_LOG_TOPIC;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = ShumaichApplication.class)
@ContextConfiguration(initializers = IntegrationTestBase.Initializer.class)
public abstract class IntegrationTestBase {

    @ClassRule
    public static EmbeddedKafkaRule kafka = new EmbeddedKafkaRule(3, true, 20,
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

    protected void clearTopic(String topicName, Integer offset) {
        kafka.getEmbeddedKafka().doWithAdmin(adminClient -> {
            try {
                TopicDescription topicDescription = adminClient
                        .describeTopics(List.of(topicName))
                        .values()
                        .get(topicName)
                        .get();
                Map<TopicPartition, RecordsToDelete> recordsToDelete = topicDescription.partitions().stream()
                        .map(TopicPartitionInfo::partition)
                        .collect(Collectors.toMap(integer ->
                                new TopicPartition(topicName, integer),
                                integer -> RecordsToDelete.beforeOffset(offset))
                        );
                adminClient.deleteRecords(recordsToDelete).lowWatermarks().forEach((topicPartition, deletedRecordsKafkaFuture) -> {
                    try {
                        deletedRecordsKafkaFuture.get();
                    } catch (Exception ignored) {}
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

}
