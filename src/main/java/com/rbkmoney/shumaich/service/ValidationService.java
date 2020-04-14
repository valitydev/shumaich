package com.rbkmoney.shumaich.service;

import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingBatch;
import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import com.rbkmoney.shumaich.exception.AccountsHaveDifferentCurrenciesException;
import com.rbkmoney.shumaich.exception.AccountsInPostingsAreEqualException;
import com.rbkmoney.shumaich.exception.CurrencyInPostingsNotConsistentException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ValidationService {

    private final PlanService planService;

    public void validatePostings(PostingPlanChange postingPlanChange) {
        validatePostings(postingPlanChange.getBatch().getPostings());
    }

    public void validatePostings(PostingPlan postingPlan) {
        for (PostingBatch postingBatch : postingPlan.getBatchList()) {
            validatePostings(postingBatch.getPostings());
        }
    }

    private void validatePostings(List<Posting> postings) {
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

    public void validateFinalOp(PostingPlanOperation postingPlanOperation) {
        planService.checkIfHoldAndChecksumMatch(postingPlanOperation);
    }
}
