package com.rbkmoney.shumaich.domain;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostingPlanOperation {

    private String planId;
    private List<PostingBatch> postingBatches;
    private OperationType operationType;
    private ValidationStatus validationStatus;
}
