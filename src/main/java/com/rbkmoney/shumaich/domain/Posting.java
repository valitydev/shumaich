package com.rbkmoney.shumaich.domain;

import com.rbkmoney.damsel.shumaich.Account;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Posting {
    private Account fromAccount;
    private Account toAccount;
    private Long amount;
    private String currencySymbolicCode;
    private String description;
}
