package com.rbkmoney.shumaich.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Plan {
    private String planId;
    private Map<Long, PlanBatch> batches;

    public PlanBatch getBatch(Long key) {
        return batches.get(key);
    }

    public PlanBatch addBatch(Long key, PlanBatch planBatch) {
        batches.put(key, planBatch);
        return planBatch;
    }

}
