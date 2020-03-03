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
    private Set<Long> sequenceArrived;
    private Long sequencesTotal;

    public boolean containsSequenceValue(Long num) {
        return sequenceArrived.contains(num);
    }

    public void addSequence(Long seq) {
        sequenceArrived.add(seq);
    }
}
