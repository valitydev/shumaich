package com.rbkmoney.shumaich.kafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.exception.ConversionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
public class OperationLogDeserializer implements Deserializer<OperationLog> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public OperationLog deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, OperationLog.class);
        } catch (IOException e) {
            log.error("Error in deserializer", e);
            throw new ConversionException(e);
        }
    }

}
