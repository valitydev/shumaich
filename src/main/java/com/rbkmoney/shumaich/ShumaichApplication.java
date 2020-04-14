package com.rbkmoney.shumaich;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;


@ServletComponentScan
@SpringBootApplication
public class ShumaichApplication extends SpringApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShumaichApplication.class, args);
    }

}
