package com.rbkmoney.shumaich.converter;

import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.RequestLog;
import org.springframework.core.convert.converter.Converter;

import java.util.List;

public class RequestLogToOperationLogListConverter implements Converter<RequestLog, List<OperationLog>> {

    @Override
    public List<OperationLog> convert(RequestLog source) {
        //todo
        return null;
    }
}
