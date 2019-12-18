package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.domain.RequestLog;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class PostingPlanToRequestLogConverter {

    private final PostingBatchDamselToPostingBatchConverter converter;

    public RequestLog convert(PostingPlan source, OperationType operationType) {
        return RequestLog.builder()
                .planId(source.id)
                .operationType(operationType)
                .postingBatches(source.batch_list.stream().map(converter::convert).collect(Collectors.toList()))
                .build();
    }
}
