package com.rbkmoney.shumaich.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Posting {
    private Long fromId;
    private Long toId;
    private Long amount;
    private String currencySymCode;
    private String description;
}
