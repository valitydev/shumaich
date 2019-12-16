package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.domain.RequestLog;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class PostingPlanChangeToRequestLogConverter {

    private final PostingBatchDamselToPostingBatchConverter converter;

    public RequestLog convert(PostingPlanChange source) {
        return RequestLog.builder()
                .planId(source.id)
                .operationType(OperationType.HOLD)
                .postingBatches(List.of(source.batch).stream().map(converter::convert).collect(Collectors.toList()))
                .build();
    }
}
