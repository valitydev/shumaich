package com.rbkmoney.shumaich.service;

import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingBatch;
import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.domain.*;
import com.rbkmoney.shumaich.exception.AccountsHaveDifferentCurrenciesException;
import com.rbkmoney.shumaich.exception.AccountsInPostingsAreEqualException;
import com.rbkmoney.shumaich.exception.CurrencyInPostingsNotConsistentException;
import com.rbkmoney.shumaich.exception.HoldChecksumMismatchException;
import com.rbkmoney.shumaich.utils.HashUtils;
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

    public ValidationStatus validateFinalOp(PostingPlanOperation postingPlanOperation) {
        Plan plan = planService.getPlan(postingPlanOperation.getPlanId(), OperationType.HOLD);
        return validatePreviousHold(plan, postingPlanOperation);
    }

    public ValidationStatus validatePreviousHold(Plan plan, PostingPlanOperation postingPlanOperation) {
        if (plan == null) {
            return ValidationStatus.HOLD_NOT_EXIST;
        }

        for (com.rbkmoney.shumaich.domain.PostingBatch postingBatch : postingPlanOperation.getPostingBatches()) {
            PlanBatch storedBatch = plan.getBatch(postingBatch.getId());
            if (storedBatch == null) {
                return ValidationStatus.HOLD_NOT_EXIST;
            }
            if (!HashUtils.areHashesEqual(postingBatch.getPostings(), storedBatch.getBatchHash())) {
                throw new HoldChecksumMismatchException();
            }
        }

        return null;
    }
}
