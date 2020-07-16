package com.rbkmoney.shumaich.domain;


import com.rbkmoney.damsel.shumaich.OperationType;
import com.rbkmoney.damsel.shumaich.ValidationError;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostingPlanOperation {

    private String planId;
    private List<PostingBatch> postingBatches;
    private OperationType operationType;
    private ValidationError validationError;
    private LocalDateTime creationTime;
}
