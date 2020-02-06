package com.rbkmoney.shumaich.utils;

import com.rbkmoney.damsel.shumpune.VectorClockState;

import java.nio.ByteBuffer;

public class VectorClockSerializer {

    public static VectorClockState serialize(String clock) {
        return new VectorClockState(ByteBuffer.wrap(clock.getBytes()));
    }

    public static String deserialize(VectorClockState clock) {
        return new String(clock.getState());
    }

}
