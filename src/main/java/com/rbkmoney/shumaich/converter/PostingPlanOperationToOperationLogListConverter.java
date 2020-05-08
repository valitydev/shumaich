package com.rbkmoney.shumaich.converter;

import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.Posting;
import com.rbkmoney.shumaich.domain.PostingBatch;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import com.rbkmoney.shumaich.utils.HashUtils;
import com.rbkmoney.shumaich.utils.WoodyTraceUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.stream.LongStream;

@Component
@RequiredArgsConstructor
public class PostingPlanOperationToOperationLogListConverter {

    public List<OperationLog> convert(PostingPlanOperation source) {
        List<OperationLog> operationLogs = new ArrayList<>();
        // Each posting will be divided into 2 operations
        long totalOperations = 2 * source.getPostingBatches().stream()
                .mapToLong(postingBatch -> postingBatch.getPostings().size())
                .sum();
        PrimitiveIterator.OfLong sequenceId = LongStream.range(0, totalOperations).iterator();
        for (PostingBatch postingBatch : source.getPostingBatches()) {
            for (Posting posting : postingBatch.getPostings()) {
                long batchHash = HashUtils.computeHash(postingBatch.getPostings());
                operationLogs.add(
                        createOperationLog(source, totalOperations, sequenceId.next(),
                                postingBatch, posting, batchHash, true)
                );
                operationLogs.add(
                        createOperationLog(source, totalOperations, sequenceId.next(),
                                postingBatch, posting, batchHash, false)
                );
            }
        }

        addLogInfo(operationLogs);

        return operationLogs;
    }

    private void addLogInfo(List<OperationLog> operationLogs) {
        final String parentId = WoodyTraceUtils.getParentId();
        final String spanId = WoodyTraceUtils.getSpanId();
        final String traceId = WoodyTraceUtils.getTraceId();

        for (OperationLog operationLog : operationLogs) {
            operationLog.setSpanId(spanId);
            operationLog.setParentId(parentId);
            operationLog.setTraceId(traceId);
        }
    }

    private OperationLog createOperationLog(PostingPlanOperation source,
                                            long totalOperations,
                                            Long sequenceId,
                                            PostingBatch postingBatch,
                                            Posting posting,
                                            long batchHash,
                                            boolean first) {
        return OperationLog.builder()
                .planId(source.getPlanId())
                .batchId(postingBatch.getId())
                .operationType(source.getOperationType())
                .account(first
                        ? posting.getToAccount()
                        : posting.getFromAccount())
                .amountWithSign(first
                        ? posting.getAmount()
                        : Math.negateExact(posting.getAmount()))
                .currencySymbolicCode(posting.getCurrencySymbolicCode())
                .description(posting.getDescription())
                .sequence(sequenceId)
                .total(totalOperations)
                .batchHash(batchHash)
                .validationStatus(source.getValidationStatus())
                .build();
    }
}
