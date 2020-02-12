package com.rbkmoney.shumaich.converter;

import com.rbkmoney.shumaich.domain.Account;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import com.rbkmoney.shumaich.helpers.TestData;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PostingPlanOperationToOperationLogListConverterTest {

    PostingPlanOperationToOperationLogListConverter converter = new PostingPlanOperationToOperationLogListConverter();

    @Test
    public void noGapsInSequence() {
        PostingPlanOperation postingPlanOperation = TestData.postingPlanOperation();
        List<OperationLog> operationLogs = converter.convert(postingPlanOperation);

        Assert.assertEquals(operationLogs.get(0).getTotal().longValue(), operationLogs.size());

        List<OperationLog> orderedOperationLogs = operationLogs.stream()
                .sorted(Comparator.comparingLong(OperationLog::getSequence))
                .collect(Collectors.toList());

        for (int i = 0; i < orderedOperationLogs.size(); i++) {
            OperationLog operationLog = orderedOperationLogs.get(i);
            Assert.assertEquals(i, operationLog.getSequence().intValue());
            Assert.assertEquals(orderedOperationLogs.size(), operationLog.getTotal().intValue());
        }
    }

    @Test
    public void equalMoneyDistribution() {
        PostingPlanOperation postingPlanOperation = TestData.postingPlanOperation();
        List<OperationLog> operationLogs = converter.convert(postingPlanOperation);

        Map<Account, Long> postingPlanMoneyMap = new HashMap<>();

        postingPlanOperation.getPostingBatches().stream()
                .flatMap(postingBatch -> postingBatch.getPostings().stream())
                .forEach(posting -> {
                    if (postingPlanMoneyMap.containsKey(posting.getToAccount())) {
                        postingPlanMoneyMap.put(posting.getToAccount(),
                                postingPlanMoneyMap.get(posting.getToAccount()) + posting.getAmount());
                    }
                    if (postingPlanMoneyMap.containsKey(posting.getFromAccount())) {
                        postingPlanMoneyMap.put(posting.getFromAccount(),
                                postingPlanMoneyMap.get(posting.getFromAccount()) + Math.negateExact(posting.getAmount()));
                    }
                    postingPlanMoneyMap.putIfAbsent(posting.getToAccount(), posting.getAmount());
                    postingPlanMoneyMap.putIfAbsent(posting.getFromAccount(), Math.negateExact(posting.getAmount()));
                });

        Map<Account, Long> operationLogMoneyMap = operationLogs.stream()
                .collect(Collectors.groupingBy(OperationLog::getAccount))
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        operationLogEntry -> operationLogEntry.getValue().stream()
                                .mapToLong(OperationLog::getAmountWithSign)
                                .reduce(0, Long::sum)));

        Assert.assertEquals(postingPlanMoneyMap, operationLogMoneyMap);
    }
}
