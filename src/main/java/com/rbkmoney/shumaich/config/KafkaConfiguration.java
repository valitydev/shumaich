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

    private static final String PKCS_12 = "PKCS12";
    private static final String SSL = "SSL";
    private static final String NONE = "none";
    private static final String EARLIEST = "earliest";

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.ssl.server-password}")
    private String serverStorePassword;

    @Value("${kafka.ssl.server-keystore-location}")
    private String serverStoreCertPath;

    @Value("${kafka.ssl.keystore-password}")
    private String keyStorePassword;

    @Value("${kafka.ssl.key-password}")
    private String keyPassword;

    @Value("${kafka.ssl.keystore-location}")
    private String clientStoreCertPath;

    @Value("${kafka.ssl.enable}")
    private Boolean kafkaSslEnable;

    @Value("${kafka.topics.partitions-per-thread}")
    private Integer partitionsPerThread;

    @Value("${kafka.topics.polling-timeout}")
    private Long pollingTimeout;

    @Value("${kafka.topics.request-log-name}")
    private String requestLogTopicName;

    @Value("${kafka.topics.operation-log-name}")
    private String operationLogTopicName;

    public Map<String, Object> consumerConfig() {
        //todo enrich
        final Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.putAll(sslConfigure(kafkaSslEnable, serverStoreCertPath, serverStorePassword,
                clientStoreCertPath, keyStorePassword, keyPassword));
        return props;
    }

    private Map<String, Object> commonProducerProps() {
        //todo enrich
        final Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.putAll(sslConfigure(kafkaSslEnable, serverStoreCertPath, serverStorePassword,
                clientStoreCertPath, keyStorePassword, keyPassword));
        return props;
    }

    @Bean
    public KafkaTemplate<String, RequestLog> requestLogKafkaTemplate() {
        Map<String, Object> configs = commonProducerProps();
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RequestLogSerializer.class);
        KafkaTemplate<String, RequestLog> kafkaTemplate = new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(configs), true);
        kafkaTemplate.setDefaultTopic(requestLogTopicName);
        return kafkaTemplate;
    }

    @Bean
    public KafkaTemplate<String, OperationLog> operationLogKafkaTemplate() {
        Map<String, Object> configs = commonProducerProps();
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
        props.putAll(sslConfigure(kafkaSslEnable, serverStoreCertPath, serverStorePassword,
                clientStoreCertPath, keyStorePassword, keyPassword));
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

    public static Map<String, Object> sslConfigure(Boolean kafkaSslEnable, String serverStoreCertPath, String serverStorePassword,
                                                   String clientStoreCertPath, String keyStorePassword, String keyPassword) {
        Map<String, Object> configProps = new HashMap<>();
        if (kafkaSslEnable) {
            configProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SSL);
            configProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, new File(serverStoreCertPath).getAbsolutePath());
            configProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, serverStorePassword);
            configProps.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, PKCS_12);
            configProps.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, PKCS_12);
            configProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, new File(clientStoreCertPath).getAbsolutePath());
            configProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
            configProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        }
        return configProps;
    }

}
