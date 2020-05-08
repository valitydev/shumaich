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
    private Long sequence;
    private Long total;
    private Long batchHash;
    private ValidationStatus validationStatus;

    //we need to save this to Kafka for better debugging
    private String spanId;
    private String parentId;
    private String traceId;
}
