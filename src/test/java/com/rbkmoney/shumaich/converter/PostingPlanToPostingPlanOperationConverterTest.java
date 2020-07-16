package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumaich.OperationType;
import com.rbkmoney.damsel.shumaich.PostingPlan;
import com.rbkmoney.shumaich.domain.Posting;
import com.rbkmoney.shumaich.domain.PostingBatch;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import com.rbkmoney.shumaich.helpers.TestData;
import org.junit.Test;

import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PostingPlanToPostingPlanOperationConverterTest {

    PostingPlanToPostingPlanOperationConverter converter = new PostingPlanToPostingPlanOperationConverter(
            new PostingBatchDamselToPostingBatchConverter(
                    new PostingDamselToPostingConverter()
            )
    );

    @Test
    public void convert() {
        PostingPlan source = TestData.postingPlan();
        PostingPlanOperation plan = converter.convert(source, OperationType.HOLD);

        assertEquals(LocalDateTime.parse("2016-03-22T06:12:27"), plan.getCreationTime());
        assertEquals("plan", plan.getPlanId());
        assertEquals(OperationType.HOLD, plan.getOperationType());

        for (int i = 0; i < plan.getPostingBatches().size(); i++) {
            PostingBatch postingBatch = plan.getPostingBatches().get(i);
            assertEquals(source.batch_list.get(i).id, postingBatch.getId().longValue());
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
