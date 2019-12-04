package com.rbkmoney.shumaich.config;

import com.rbkmoney.shumaich.domain.RequestLog;
import com.rbkmoney.shumaich.kafka.serde.RequestLogSerializer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaConfiguration {

    private static final String PKCS_12 = "PKCS12";
    private static final String SSL = "SSL";
    private static final String NONE = "none";

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
    private boolean kafkaSslEnable;

    public Map<String, Object> consumerConfig() {
        //todo enrich
        final Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, NONE);
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
    public KafkaProducer<String, RequestLog> kafkaRequestLogProducer() {
        Map<String, Object> configs = commonProducerProps();
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RequestLogSerializer.class);
        return new KafkaProducer<>(configs);
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
