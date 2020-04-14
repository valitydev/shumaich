package com.rbkmoney.shumaich.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
    private Long batchHash;

    public boolean containsSequenceValue(Long num) {
        return sequencesArrived.contains(num);
    }

    public void addSequence(Long seq) {
        sequencesArrived.add(seq);
    }

    @JsonIgnore
    public boolean isCompleted() {
        return sequencesArrived.size() == sequencesTotal;
    }
}
