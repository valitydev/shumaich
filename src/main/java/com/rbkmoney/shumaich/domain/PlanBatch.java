package com.rbkmoney.shumaich.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PlanBatch {
    private Set<Long> sequencesArrived;
    private Long sequencesTotal;

    public boolean containsSequenceValue(Long num) {
        return sequencesArrived.contains(num);
    }

    public void addSequence(Long seq) {
        sequencesArrived.add(seq);
    }
}
