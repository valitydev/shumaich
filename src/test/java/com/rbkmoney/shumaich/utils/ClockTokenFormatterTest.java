package com.rbkmoney.shumaich.utils;

import com.rbkmoney.shumaich.TestData;
import org.junit.Assert;
import org.junit.Test;

public class ClockTokenFormatterTest {

    @Test
    public void serialize() {
        Assert.assertEquals("plan_hold", ClockTokenFormatter.getClock(TestData.requestLog()));
    }

}
