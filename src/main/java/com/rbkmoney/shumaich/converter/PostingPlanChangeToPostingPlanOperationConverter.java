package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumaich.OperationType;
import com.rbkmoney.damsel.shumaich.PostingPlanChange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class PostingPlanChangeToPostingPlanOperationConverter {

    private final PostingBatchDamselToPostingBatchConverter converter;

    public PostingPlanOperation convert(PostingPlanChange source) {
        return PostingPlanOperation.builder()
                .planId(source.getId())
                .postingBatches(List.of(converter.convert(source.getBatch())))
                .creationTime(TypeUtil.stringToLocalDateTime(source.getCreationTime()))
                .operationType(OperationType.HOLD)
                .build();
    }
}
