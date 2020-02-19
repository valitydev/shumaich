package com.rbkmoney.shumaich.utils;

import com.rbkmoney.damsel.shumpune.VectorClock;

import java.nio.ByteBuffer;

public class VectorClockSerde {

    public static VectorClock serialize(String clock) {
        return new VectorClock(ByteBuffer.wrap(clock.getBytes()));
    }

    public static String deserialize(VectorClock clock) {
        return new String(clock.getState());
    }

}
