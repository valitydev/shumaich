package com.rbkmoney.shumaich.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OperationLog {
    private String planId;
    private Long batchId;
    private OperationType operationType;
    private Account account;
    private Long amountWithSign;
    private String currencySymbolicCode;
    private String description;
    //todo remove?
    private String creationTime;
    private Long sequence;
    private Long total;
}
