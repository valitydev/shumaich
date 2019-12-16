package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.shumaich.TestData;
import org.junit.Assert;
import org.junit.Test;

public class PostingDamselToPostingConverterTest {

    PostingDamselToPostingConverter converter = new PostingDamselToPostingConverter();

    @Test
    public void conversion() {
        Posting postingDamsel = TestData.postingDamsel();
        var posting = converter.convert(postingDamsel);

        Assert.assertEquals(postingDamsel.amount, posting.getAmount());
        Assert.assertEquals(postingDamsel.from_id, posting.getFromId());
        Assert.assertEquals(postingDamsel.to_id, posting.getToId());
        Assert.assertEquals(postingDamsel.currency_sym_code, posting.getCurrencySymCode());
        Assert.assertEquals(postingDamsel.description, posting.getDescription());
    }

}
