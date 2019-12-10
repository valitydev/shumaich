package com.rbkmoney.shumaich.kafka.serde;

import com.rbkmoney.shumaich.domain.OperationLog;
import org.springframework.kafka.support.serializer.JsonDeserializer;

public class OperationLogDeserializer extends JsonDeserializer<OperationLog> {}
