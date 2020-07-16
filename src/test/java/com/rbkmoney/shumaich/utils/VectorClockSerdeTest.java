package com.rbkmoney.shumaich.utils;

import com.rbkmoney.damsel.shumaich.VectorClock;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VectorClockSerdeTest {

    public static final String CLOCK_TEST = "plan_operation_batch";

    @Test
    public void serialize() {
        VectorClock clock = VectorClockSerde.serialize(CLOCK_TEST);

        Assert.assertNotNull(clock);

        String deserializedClock = VectorClockSerde.deserialize(clock);

        assertEquals(CLOCK_TEST, deserializedClock);
    }

}
