package com.rbkmoney.shumaich.kafka.serde;

import com.rbkmoney.damsel.shumaich.OperationLog;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.Map;

@Slf4j
public class OperationLogDeserializer implements Deserializer<OperationLog> {

    private final ThreadLocal<TDeserializer> tDeserializerThreadLocal = ThreadLocal.withInitial(
            () -> new TDeserializer(
                    new TBinaryProtocol.Factory()));

    @Override
    public OperationLog deserialize(String topic, byte[] data) {
        OperationLog operationLog = new OperationLog();

        try {
            tDeserializerThreadLocal.get().deserialize(operationLog, data);
        } catch (Exception e) {
            log.error("Deserialization error. OperationLog data: {} ", data, e);
        }

        return operationLog;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }
}
