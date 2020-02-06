package com.rbkmoney.shumaich.converter;

import com.rbkmoney.shumaich.domain.Posting;
import org.springframework.stereotype.Component;

@Component
public class PostingDamselToPostingConverter {

    public Posting convert(com.rbkmoney.damsel.shumpune.Posting postingDamsel) {
        return Posting.builder()
                .amount(postingDamsel.amount)
//                .currencySymCode(postingDamsel.currency_sym_code)
                .description(postingDamsel.description)
//                .fromId(postingDamsel.from_id)
//                .toId(postingDamsel.to_id)
                .build();
    }

}
