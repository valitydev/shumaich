package com.rbkmoney.shumaich.service;

import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.exception.AccountsHaveDifferentCurrenciesException;
import com.rbkmoney.shumaich.exception.AccountsInPostingsAreEqualException;
import com.rbkmoney.shumaich.exception.CurrencyInPostingsNotConsistentException;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ValidationService {

    public void validatePostings(PostingPlanChange postingPlanChange) {
        List<Posting> postings = postingPlanChange.getBatch().getPostings();
        String currencySymCode = postings.get(0).getCurrencySymCode();
        for (Posting posting : postings) {
            if (!posting.getCurrencySymCode().equals(currencySymCode)) {
                throw new CurrencyInPostingsNotConsistentException();
            }
            if (posting.getFromAccount().getId().equals(posting.getToAccount().getId())) {
                throw new AccountsInPostingsAreEqualException();
            }
            if (!posting.getFromAccount().getCurrencySymCode().equals(posting.getToAccount().getCurrencySymCode())) {
                throw new AccountsHaveDifferentCurrenciesException();
            }
        }
    }
}
