package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumpune.PostingBatch;
import com.rbkmoney.shumaich.TestData;
import org.junit.Assert;
import org.junit.Test;

public class PostingBatchDamselToPostingBatchConverterTest {

    PostingBatchDamselToPostingBatchConverter converter = new PostingBatchDamselToPostingBatchConverter(
            new PostingDamselToPostingConverter()
    );

    @Test
    public void conversion() {
        PostingBatch postingBatchDamsel = TestData.postingBatchDamsel();
        var postingBatch = converter.convert(postingBatchDamsel);

        Assert.assertEquals(postingBatchDamsel.id, postingBatch.getId());
        Assert.assertEquals(postingBatchDamsel.getPostings().size(), postingBatch.getPostings().size());
    }

}
