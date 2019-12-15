package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.domain.RequestLog;
import org.springframework.stereotype.Component;

@Component
public class PostingPlanToRequestLogConverter {

    public RequestLog convert(PostingPlan source, OperationType operationType) {
        return RequestLog.builder()
                .planId(source.id)
                .operationType(operationType)
                .postingBatches(source.batch_list)
                .build();
    }
}
