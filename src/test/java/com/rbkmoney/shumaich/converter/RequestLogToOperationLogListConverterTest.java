package com.rbkmoney.shumaich.converter;

import com.rbkmoney.shumaich.TestData;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.RequestLog;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RequestLogToOperationLogListConverterTest {

    RequestLogToOperationLogListConverter converter = new RequestLogToOperationLogListConverter();

    @Test
    public void noGapsInSequence() {
        RequestLog requestLog = TestData.requestLog();
        List<OperationLog> operationLogs = converter.convert(requestLog);

        Assert.assertEquals(operationLogs.get(0).getTotal().intValue(), operationLogs.size());

        List<OperationLog> orderedOperationLogs = operationLogs.stream()
                .sorted(Comparator.comparingInt(OperationLog::getSequence))
                .collect(Collectors.toList());

        for (int i = 0; i < orderedOperationLogs.size(); i++) {
            OperationLog operationLog = orderedOperationLogs.get(i);
            Assert.assertEquals(i, operationLog.getSequence().intValue());
            Assert.assertEquals(orderedOperationLogs.size(), operationLog.getTotal().intValue());
        }
    }

    @Test
    public void equalMoneyDistribution() {
        RequestLog requestLog = TestData.requestLog();
        List<OperationLog> operationLogs = converter.convert(requestLog);

        Map<Long, Long> requestLogMoneyMap = new HashMap<>();

        requestLog.getPostingBatches().stream()
                .flatMap(postingBatch -> postingBatch.getPostings().stream())
                .forEach(posting -> {
                    if (requestLogMoneyMap.containsKey(posting.getToId())) {
                        requestLogMoneyMap.put(posting.getToId(),
                                requestLogMoneyMap.get(posting.getToId()) + posting.getAmount());
                    }
                    if (requestLogMoneyMap.containsKey(posting.getFromId())) {
                        requestLogMoneyMap.put(posting.getFromId(),
                                requestLogMoneyMap.get(posting.getFromId()) + Math.negateExact(posting.getAmount()));
                    }
                    requestLogMoneyMap.putIfAbsent(posting.getToId(), posting.getAmount());
                    requestLogMoneyMap.putIfAbsent(posting.getFromId(), Math.negateExact(posting.getAmount()));
                });

        Map<Long, Long> operationLogMoneyMap = operationLogs.stream()
                .collect(Collectors.groupingBy(OperationLog::getAccount))
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        operationLogEntry -> operationLogEntry.getValue().stream()
                                .mapToLong(OperationLog::getAmountWithSign)
                                .reduce(0, Long::sum)));

        Assert.assertEquals(requestLogMoneyMap, operationLogMoneyMap);
    }
}
