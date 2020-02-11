package com.rbkmoney.shumaich.converter;

import com.rbkmoney.shumaich.domain.Account;
import com.rbkmoney.shumaich.domain.Posting;
import org.springframework.stereotype.Component;

@Component
public class PostingDamselToPostingConverter {

    public Posting convert(com.rbkmoney.damsel.shumpune.Posting postingDamsel) {
        return Posting.builder()
                .amount(postingDamsel.amount)
                .currencySymbolicCode(postingDamsel.currency_sym_code)
                .description(postingDamsel.description)
                .fromAccount(new Account(postingDamsel.from_account.id, postingDamsel.from_account.currency_sym_code))
                .toAccount(new Account(postingDamsel.to_account.id, postingDamsel.to_account.currency_sym_code))
                .build();
    }

}
