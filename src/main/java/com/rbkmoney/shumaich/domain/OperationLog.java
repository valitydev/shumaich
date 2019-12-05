package com.rbkmoney.shumaich.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
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
