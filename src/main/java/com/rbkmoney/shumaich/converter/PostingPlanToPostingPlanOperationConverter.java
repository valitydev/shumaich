package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumaich.PostingPlan;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class PostingPlanToPostingPlanOperationConverter {

    private final PostingBatchDamselToPostingBatchConverter converter;

    public PostingPlanOperation convert(PostingPlan source, OperationType operationType) {
        return PostingPlanOperation.builder()
                .planId(source.getId())
                .postingBatches(source.batch_list.stream().map(converter::convert).collect(Collectors.toList()))
                .operationType(operationType)
                .build();
    }

}
