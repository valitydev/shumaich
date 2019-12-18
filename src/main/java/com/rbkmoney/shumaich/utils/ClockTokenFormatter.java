package com.rbkmoney.shumaich.utils;

import com.rbkmoney.shumaich.domain.RequestLog;

public class ClockTokenFormatter {

    private static final String CLOCK_FORMAT = "%s_%s";

    public static String getClock(RequestLog requestLog) {
        return String.format(CLOCK_FORMAT, requestLog.getPlanId(), requestLog.getOperationType());
    }

}
