package com.rbkmoney.shumaich.kafka.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.exception.ConversionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class OperationLogSerializer implements Serializer<OperationLog> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, OperationLog data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            log.error("Error in serializer", e);
            throw new ConversionException(e);
        }
    }
}
