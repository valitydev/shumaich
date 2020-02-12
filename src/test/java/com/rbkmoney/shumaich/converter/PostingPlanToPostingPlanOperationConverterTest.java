package com.rbkmoney.shumaich.converter;

import com.rbkmoney.shumaich.domain.Posting;
import com.rbkmoney.shumaich.domain.PostingBatch;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import com.rbkmoney.shumaich.helpers.TestData;
import org.junit.Assert;
import org.junit.Test;

import static com.rbkmoney.shumaich.domain.OperationType.HOLD;

public class PostingPlanToPostingPlanOperationConverterTest {

    PostingPlanToPostingPlanOperationConverter converter = new PostingPlanToPostingPlanOperationConverter(
            new PostingBatchDamselToPostingBatchConverter(
                    new PostingDamselToPostingConverter()
            )
    );

    @Test
    public void convert() {
        PostingPlanOperation plan = converter.convert(TestData.postingPlan(), HOLD);
        Assert.assertEquals("plan", plan.getPlanId());
        Assert.assertEquals(HOLD, plan.getOperationType());
        for (PostingBatch postingBatch : plan.getPostingBatches()) {
            Assert.assertEquals(1L, postingBatch.getId().longValue());
            for (Posting posting : postingBatch.getPostings()) {
                Assert.assertNotNull(posting.getAmount());
                Assert.assertNotNull(posting.getFromAccount());
                Assert.assertNotNull(posting.getToAccount());
                Assert.assertNotNull(posting.getCurrencySymbolicCode());
                Assert.assertNotNull(posting.getDescription());
            }
        }
    }

}
