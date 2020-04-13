package com.rbkmoney.shumaich;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;


@ServletComponentScan
@EnableScheduling
public class ShumaichApplication extends SpringApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShumaichApplication.class, args);
    }

}
