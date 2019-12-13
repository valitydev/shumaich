package com.rbkmoney.shumaich.kafka.serde;

import com.rbkmoney.shumaich.domain.RequestLog;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class RequestLogSerializer extends JsonSerializer<RequestLog> {
}
