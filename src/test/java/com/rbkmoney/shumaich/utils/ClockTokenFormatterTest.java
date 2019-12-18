package com.rbkmoney.shumaich.utils;

import com.rbkmoney.shumaich.helpers.TestData;
import org.junit.Assert;
import org.junit.Test;

public class ClockTokenFormatterTest {

    @Test
    public void serialize() {
        Assert.assertEquals("plan_HOLD", ClockTokenFormatter.getClock(TestData.requestLog()));
    }

}
