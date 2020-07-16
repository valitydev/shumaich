package com.rbkmoney.shumaich.kafka.serde;

import com.rbkmoney.damsel.shumaich.OperationLog;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class OperationLogSerializer extends JsonSerializer<OperationLog> {}
