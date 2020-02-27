package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.shumaich.helpers.TestData;
import org.junit.Assert;
import org.junit.Test;

public class PostingDamselToPostingConverterTest {

    PostingDamselToPostingConverter converter = new PostingDamselToPostingConverter();

    @Test
    public void conversion() {
        Posting postingDamsel = TestData.postingDamsel();
        var posting = converter.convert(postingDamsel);

        Assert.assertEquals(postingDamsel.amount, posting.getAmount().longValue());
        Assert.assertEquals(postingDamsel.from_account.currency_sym_code, posting.getFromAccount().getCurrencySymbolicCode());
        Assert.assertEquals(postingDamsel.from_account.id, posting.getFromAccount().getId());
        Assert.assertEquals(postingDamsel.to_account.currency_sym_code, posting.getToAccount().getCurrencySymbolicCode());
        Assert.assertEquals(postingDamsel.to_account.id, posting.getToAccount().getId());
        Assert.assertEquals(postingDamsel.currency_sym_code, posting.getCurrencySymbolicCode());
        Assert.assertEquals(postingDamsel.description, posting.getDescription());
    }

}
