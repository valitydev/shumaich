package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumaich.OperationType;
import com.rbkmoney.shumaich.domain.Posting;
import com.rbkmoney.shumaich.domain.PostingBatch;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import com.rbkmoney.shumaich.helpers.TestData;
import org.junit.Test;

import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PostingPlanChangeToPostingPlanOperationConverterTest {

    PostingPlanChangeToPostingPlanOperationConverter converter = new PostingPlanChangeToPostingPlanOperationConverter(
            new PostingBatchDamselToPostingBatchConverter(
                    new PostingDamselToPostingConverter()
            )
    );

    @Test
    public void convert() {
        PostingPlanOperation plan = converter.convert(TestData.postingPlanChange());

        assertEquals(LocalDateTime.parse("2016-03-22T06:12:27"), plan.getCreationTime());
        assertEquals("plan", plan.getPlanId());
        assertEquals(OperationType.HOLD, plan.getOperationType());

        for (PostingBatch postingBatch : plan.getPostingBatches()) {
            assertEquals(1L, postingBatch.getId().longValue());
            for (Posting posting : postingBatch.getPostings()) {
                assertNotNull(posting.getAmount());
                assertNotNull(posting.getFromAccount());
                assertNotNull(posting.getToAccount());
                assertNotNull(posting.getCurrencySymbolicCode());
                assertNotNull(posting.getDescription());
            }
        }
    }

}
