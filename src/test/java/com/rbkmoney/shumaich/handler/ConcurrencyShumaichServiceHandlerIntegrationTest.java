package com.rbkmoney.shumaich.handler;

import com.rbkmoney.shumaich.IntegrationTestBase;
import lombok.extern.slf4j.Slf4j;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.test.annotation.DirtiesContext;

@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@Ignore
public class ConcurrencyShumaichServiceHandlerIntegrationTest extends IntegrationTestBase {

    @Test
    public void concurrentHoldsTest() {}

    @Test
    public void concurrentFlowTest() {}

}
