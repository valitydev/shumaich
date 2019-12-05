package com.rbkmoney.shumaich.domain;

import com.rbkmoney.damsel.shumpune.PostingBatch;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RequestLog {
    private String planId;
    private OperationType operationType;
    private List<PostingBatch> postingBatches;
}
