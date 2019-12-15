package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.domain.RequestLog;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class PostingPlanChangeToRequestLogConverter {

    public RequestLog convert(PostingPlanChange source) {
        return RequestLog.builder()
                .planId(source.id)
                .operationType(OperationType.HOLD)
                .postingBatches(List.of(source.batch))
                .build();
    }
}
