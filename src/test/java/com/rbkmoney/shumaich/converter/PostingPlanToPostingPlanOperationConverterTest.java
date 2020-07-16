package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumaich.PostingPlan;
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
        PostingPlan source = TestData.postingPlan();
        PostingPlanOperation plan = converter.convert(source, HOLD);
        Assert.assertEquals("plan", plan.getPlanId());
        Assert.assertEquals(HOLD, plan.getOperationType());
        for (int i = 0; i < plan.getPostingBatches().size(); i++) {
            PostingBatch postingBatch = plan.getPostingBatches().get(i);
            Assert.assertEquals(source.batch_list.get(i).id, postingBatch.getId().longValue());
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
