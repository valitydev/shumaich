package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class PostingPlanChangeToPostingPlanOperationConverter {

    private final PostingBatchDamselToPostingBatchConverter converter;

    PostingPlanOperation convert(PostingPlanChange source, OperationType operationType) {
        return PostingPlanOperation.builder()
                .planId(source.getId())
                .postingBatches(List.of(converter.convert(source.batch)))
                .operationType(operationType)
                .build();
    }
}
