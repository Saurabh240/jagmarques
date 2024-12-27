package com.majoozilla.routesapplication;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class RoutesApplication {

    public static void main(String[] args) {
        SpringApplication.run(RoutesApplication.class, args);
    }

}
