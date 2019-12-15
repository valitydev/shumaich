package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingBatch;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.domain.RequestLog;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.stream.IntStream;

@Component
public class RequestLogToOperationLogListConverter {

    public List<OperationLog> convert(RequestLog source) {
        List<OperationLog> operationLogs = new ArrayList<>();
        // Each posting will be divided into 2 operations
        int totalOperations = 2 * source.getPostingBatches().stream().mapToInt(PostingBatch::getPostingsSize).sum();
        PrimitiveIterator.OfInt sequenceId = IntStream.range(0, totalOperations).iterator();
        Instant creationTime = Instant.now();
        for (PostingBatch postingBatch : source.getPostingBatches()) {
            for (Posting posting : postingBatch.getPostings()) {
                operationLogs.add(
                        createOperationLog(source, totalOperations, sequenceId.next(),
                                creationTime, postingBatch, posting, true)
                );
                operationLogs.add(
                        createOperationLog(source, totalOperations, sequenceId.next(),
                                creationTime, postingBatch, posting, false)
                );
            }
        }
        return operationLogs;
    }

    private OperationLog createOperationLog(RequestLog source,
                                            int totalOperations,
                                            Integer sequenceId,
                                            Instant creationTime,
                                            PostingBatch postingBatch,
                                            Posting posting,
                                            boolean first) {
        return OperationLog.builder()
                .planId(source.getPlanId())
                .batchId(postingBatch.getId())
                .operationType(source.getOperationType())
                .account(first
                        ? posting.getToId()
                        : posting.getFromId())
                .amountWithSign(first
                        ? posting.getAmount()
                        : Math.negateExact(posting.getAmount()))
                .currencySymbolicCode(posting.getCurrencySymCode())
                .description(posting.getDescription())
                .creationTime(creationTime)
                .sequence(sequenceId)
                .total(totalOperations)
                .build();
    }
}
