package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.shumaich.TestData;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.domain.RequestLog;
import org.junit.Assert;
import org.junit.Test;

public class PostingPlanToRequestLogConverterTest {

    PostingPlanToRequestLogConverter converter = new PostingPlanToRequestLogConverter();

    @Test
    public void conversion() {
        PostingPlan postingPlan = TestData.postingPlan();
        RequestLog requestLog = converter.convert(postingPlan, OperationType.COMMIT);

        Assert.assertEquals("plan", requestLog.getPlanId());
        Assert.assertEquals(OperationType.COMMIT, requestLog.getOperationType());
        Assert.assertEquals(postingPlan.getBatchList(), requestLog.getPostingBatches());
    }

}
