package com.majoozilla.tripsapplication;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TripsApplication {

    public static void main(String[] args) {
        SpringApplication.run(TripsApplication.class, args);
    }

}
