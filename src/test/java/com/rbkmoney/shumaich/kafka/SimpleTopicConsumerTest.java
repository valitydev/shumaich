package com.rbkmoney.shumaich.kafka;

import com.rbkmoney.shumaich.IntegrationTestBase;
import com.rbkmoney.shumaich.ShumaichApplication;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@Slf4j
@RunWith(SpringRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = RANDOM_PORT, classes = ShumaichApplication.class)
public class SimpleTopicConsumerTest extends IntegrationTestBase {

    @Test
    public void test() {

    }

}
