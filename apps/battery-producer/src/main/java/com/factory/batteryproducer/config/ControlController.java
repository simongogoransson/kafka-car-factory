package com.factory.batteryproducer.config;

import com.factory.batteryproducer.producer.BatteryEventProducer;
import com.factory.batteryproducer.scheduler.BatteryProductionScheduler;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Simple HTTP control API to pause / resume / inspect the producer.
 */
@RestController
@RequestMapping("/control")
public class ControlController {

    private final BatteryProductionScheduler scheduler;
    private final BatteryEventProducer producer;

    public ControlController(BatteryProductionScheduler scheduler, BatteryEventProducer producer) {
        this.scheduler = scheduler;
        this.producer  = producer;
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(Map.of(
                "running",         scheduler.isRunning(),
                "totalEventsSent", producer.getTotalEventsSent(),
                "service",         "battery-producer"
        ));
    }

    @PostMapping("/pause")
    public ResponseEntity<Map<String, String>> pause() {
        scheduler.pause();
        return ResponseEntity.ok(Map.of("status", "paused"));
    }

    @PostMapping("/resume")
    public ResponseEntity<Map<String, String>> resume() {
        scheduler.resume();
        return ResponseEntity.ok(Map.of("status", "running"));
    }
}

