package com.rbkmoney.shumaich.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Balance {
    private Long accountId;
    private String currencySymbolicCode;
    private Long amount;
    private Long minAmount;
    private Long maxAmount;
}
