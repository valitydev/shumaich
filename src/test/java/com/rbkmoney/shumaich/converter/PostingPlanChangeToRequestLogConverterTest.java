package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.TestData;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.domain.RequestLog;
import org.junit.Assert;
import org.junit.Test;

public class PostingPlanChangeToRequestLogConverterTest {

    PostingPlanChangeToRequestLogConverter converter = new PostingPlanChangeToRequestLogConverter();

    @Test
    public void conversion() {
        PostingPlanChange postingPlanChange = TestData.postingPlanChange();
        RequestLog requestLog = converter.convert(postingPlanChange);

        Assert.assertEquals("plan", requestLog.getPlanId());
        Assert.assertEquals(OperationType.HOLD, requestLog.getOperationType());
        Assert.assertEquals(1, requestLog.getPostingBatches().size());
        Assert.assertEquals(postingPlanChange.getBatch(), requestLog.getPostingBatches().get(0));
    }

}
