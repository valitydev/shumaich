package com.rbkmoney.shumaich.kafka.serde;

import com.rbkmoney.shumaich.domain.RequestLog;
import org.springframework.kafka.support.serializer.JsonDeserializer;

public class RequestLogDeserializer extends JsonDeserializer<RequestLog> {}
