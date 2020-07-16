package com.rbkmoney.shumaich.kafka.serde;

import com.rbkmoney.damsel.shumaich.OperationLog;
import com.rbkmoney.kafka.common.serialization.AbstractThriftDeserializer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OperationLogDeserializer extends AbstractThriftDeserializer<OperationLog> {

    @Override
    public OperationLog deserialize(String topic, byte[] data) {
        return deserialize(data, new OperationLog());
    }
}
