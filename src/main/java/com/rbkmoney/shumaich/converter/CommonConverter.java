package com.rbkmoney.shumaich.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rbkmoney.shumaich.domain.Clock;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class CommonConverter {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static <T> T fromBytes(byte[] bytes, Class<T> clazz) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        try {
            return objectMapper.readValue(bytes, clazz);
        } catch (IOException e) {
            log.error("Can't deserialize value", e);
            throw new RuntimeException("Can't deserialize value", e);
        }
    }

    public static <T> byte[] toBytes(T object) {
        try {
            return objectMapper.writeValueAsBytes(object);
        } catch (IOException e) {
            log.error("Can't serialize value", e);
            throw new RuntimeException("Can't serialize value", e);
        }
    }

    public static <T> String serialize(T object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            log.error("Error in clock JSON serialization", e);
            throw new RuntimeException(e);
        }
    }

    public static <T> T deserialize(String json, Class<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (IOException e) {
            log.error("Error in clock JSON deserialization", e);
            throw new RuntimeException(e);
        }
    }
}
