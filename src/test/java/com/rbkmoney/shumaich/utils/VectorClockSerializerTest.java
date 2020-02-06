package com.rbkmoney.shumaich.utils;

import com.rbkmoney.damsel.shumpune.VectorClockState;
import org.junit.Assert;
import org.junit.Test;

public class VectorClockSerializerTest {

    public static final String CLOCK_TEST = "plan_operation_batch";

    @Test
    public void serialize() {
        VectorClockState clock = VectorClockSerializer.serialize(CLOCK_TEST);

        Assert.assertNotNull(clock);

        String deserializedClock = VectorClockSerializer.deserialize(clock);

        Assert.assertEquals(CLOCK_TEST, deserializedClock);
    }

}
