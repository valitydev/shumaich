package com.rbkmoney.shumaich.config;

import com.rbkmoney.shumaich.domain.KafkaOffset;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfiguration {

    @Bean
    public RedisConnectionFactory redisConnectionFactory(@Value("${redis.host}") String host,
                                                         @Value("${redis.port}") Integer port) {
        //todo configure
        return new JedisConnectionFactory(new RedisStandaloneConfiguration(host, port));
    }

    @Bean
    public RedisTemplate<String, KafkaOffset> redisKafkaOffsetTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, KafkaOffset> redisKafkaOffsetTemplate = new RedisTemplate<>();
        redisKafkaOffsetTemplate.setKeySerializer(new StringRedisSerializer());
        redisKafkaOffsetTemplate.setValueSerializer(new JdkSerializationRedisSerializer());
        redisKafkaOffsetTemplate.setConnectionFactory(redisConnectionFactory);
        return redisKafkaOffsetTemplate;
    }

}
