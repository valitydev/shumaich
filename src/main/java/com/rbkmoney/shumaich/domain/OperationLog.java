package com.rbkmoney.shumaich.domain;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@Builder
public class OperationLog {
    private String planId;
    private String batchId;
    private OperationType operationType;
    private Long account;
    private Long amountWithSign;
    private String currencySymbCode;
    private String description;
    private Instant creationTime;
    private Integer sequence;
    private Integer total;
}
