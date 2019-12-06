package com.rbkmoney.shumaich.config;

import com.rbkmoney.shumaich.dao.KafkaOffsetDao;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.RequestLog;
import com.rbkmoney.shumaich.kafka.TopicConsumptionManager;
import com.rbkmoney.shumaich.kafka.serde.OperationLogDeserializer;
import com.rbkmoney.shumaich.kafka.serde.OperationLogSerializer;
import com.rbkmoney.shumaich.kafka.serde.RequestLogDeserializer;
import com.rbkmoney.shumaich.kafka.serde.RequestLogSerializer;
import com.rbkmoney.shumaich.service.Handler;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Configuration
@RequiredArgsConstructor
public class KafkaConfiguration {

    private static final String EARLIEST = "earliest";

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.topics.partitions-per-thread}")
    private Integer partitionsPerThread;

    @Value("${kafka.topics.polling-timeout}")
    private Long pollingTimeout;

    @Value("${kafka.topics.request-log-name}")
    private String requestLogTopicName;

    @Value("${kafka.topics.operation-log-name}")
    private String operationLogTopicName;

    private final KafkaSslProperties kafkaSslProperties;


    private Map<String, Object> consumerConfig() {
        //todo enrich
        final Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        configureSsl(props, kafkaSslProperties);

        return props;
    }


    private Map<String, Object> producerConfig() {
        //todo enrich
        final Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        configureSsl(props, kafkaSslProperties);

        return props;
    }

    @Bean
    public KafkaTemplate<String, RequestLog> requestLogKafkaTemplate() {
        Map<String, Object> configs = producerConfig();
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RequestLogSerializer.class);
        KafkaTemplate<String, RequestLog> kafkaTemplate = new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(configs), true);
        kafkaTemplate.setDefaultTopic(requestLogTopicName);
        return kafkaTemplate;
    }

    @Bean
    public KafkaTemplate<String, OperationLog> operationLogKafkaTemplate() {
        Map<String, Object> configs = producerConfig();
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OperationLogSerializer.class);
        KafkaTemplate<String, OperationLog> kafkaTemplate = new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(configs), true);
        kafkaTemplate.setDefaultTopic(operationLogTopicName);
        return kafkaTemplate;
    }

    @Bean
    AdminClient kafkaAdminClient() {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        configureSsl(props, kafkaSslProperties);

        return AdminClient.create(props);
    }

    @Bean
    public TopicConsumptionManager<String, RequestLog> requestLogTopicConsumptionManager(AdminClient kafkaAdminClient,
                                                                                         KafkaOffsetDao kafkaOffsetDao,
                                                                                         Handler<RequestLog> handler) throws ExecutionException, InterruptedException {
        Map<String, Object> consumerProps = consumerConfig();
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, RequestLogDeserializer.class);

        TopicDescription topicDescription = kafkaAdminClient
                .describeTopics(List.of(requestLogTopicName))
                .values()
                .get(requestLogTopicName)
                .get();

        return new TopicConsumptionManager<>(topicDescription,
                partitionsPerThread,
                consumerProps,
                kafkaOffsetDao,
                handler,
                pollingTimeout
        );
    }

    @Bean
    public TopicConsumptionManager<String, OperationLog> operationLogTopicConsumptionManager(AdminClient kafkaAdminClient,
                                                                                             KafkaOffsetDao kafkaOffsetDao,
                                                                                             Handler<OperationLog> handler) throws ExecutionException, InterruptedException {
        Map<String, Object> consumerProps = consumerConfig();
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OperationLogDeserializer.class);

        TopicDescription topicDescription = kafkaAdminClient
                .describeTopics(List.of(operationLogTopicName))
                .values()
                .get(operationLogTopicName)
                .get();

        return new TopicConsumptionManager<>(topicDescription,
                partitionsPerThread,
                consumerProps,
                kafkaOffsetDao,
                handler,
                pollingTimeout
        );
    }

    private void configureSsl(Map<String, Object> props, KafkaSslProperties kafkaSslProperties) {
        if (kafkaSslProperties.isEnabled()) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, new File(kafkaSslProperties.getTrustStoreLocation()).getAbsolutePath());
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, kafkaSslProperties.getTrustStorePassword());
            props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, kafkaSslProperties.getKeyStoreType());
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, kafkaSslProperties.getTrustStoreType());
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, new File(kafkaSslProperties.getKeyStoreLocation()).getAbsolutePath());
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, kafkaSslProperties.getKeyStorePassword());
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, kafkaSslProperties.getKeyPassword());
        }
    }

}
