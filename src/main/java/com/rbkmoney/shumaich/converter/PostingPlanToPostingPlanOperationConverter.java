package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumaich.OperationType;
import com.rbkmoney.damsel.shumaich.PostingPlan;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import static java.util.stream.Collectors.toList;

@Component
@RequiredArgsConstructor
public class PostingPlanToPostingPlanOperationConverter {

    private final PostingBatchDamselToPostingBatchConverter converter;

    public PostingPlanOperation convert(PostingPlan source, OperationType operationType) {
        return PostingPlanOperation.builder()
                .planId(source.getId())
                .postingBatches(source.getBatchList().stream().map(converter::convert).collect(toList()))
                .creationTime(TypeUtil.stringToLocalDateTime(source.getCreationTime()))
                .operationType(operationType)
                .build();
    }

}
