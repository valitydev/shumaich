package com.rbkmoney.shumaich.utils;

import com.rbkmoney.damsel.shumaich.VectorClock;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VectorClockSerde {

    public static VectorClock serialize(String clock) {
        return new VectorClock(ByteBuffer.wrap(clock.getBytes()));
    }

    public static String deserialize(VectorClock clock) {
        return new String(clock.getState());
    }

}
