package com.rbkmoney.shumaich.config;

import com.rbkmoney.shumaich.domain.KafkaOffset;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfiguration {

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        //todo configure
        return new JedisConnectionFactory();
    }

    @Bean
    public RedisTemplate<String, KafkaOffset> redisKafkaOffsetTemplate() {
        RedisTemplate<String, KafkaOffset> redisKafkaOffsetTemplate = new RedisTemplate<>();
        redisKafkaOffsetTemplate.setKeySerializer(new StringRedisSerializer());
        redisKafkaOffsetTemplate.setValueSerializer(new JdkSerializationRedisSerializer());
        redisKafkaOffsetTemplate.setConnectionFactory(redisConnectionFactory());
        return redisKafkaOffsetTemplate;
    }

}
