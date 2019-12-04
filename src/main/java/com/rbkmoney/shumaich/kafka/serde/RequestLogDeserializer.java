package com.rbkmoney.shumaich.kafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rbkmoney.shumaich.domain.RequestLog;
import com.rbkmoney.shumaich.exception.ConversionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
public class RequestLogDeserializer implements Deserializer<RequestLog> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public RequestLog deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, RequestLog.class);
        } catch (IOException e) {
            log.error("Error in deserializer", e);
            throw new ConversionException(e);
        }
    }

}
