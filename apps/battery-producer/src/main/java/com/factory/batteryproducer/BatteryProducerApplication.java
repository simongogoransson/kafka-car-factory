package com.factory.batteryproducer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
@SpringBootApplication
@EnableScheduling
public class BatteryProducerApplication {
    public static void main(String[] args) {
        SpringApplication.run(BatteryProducerApplication.class, args);
    }
}
