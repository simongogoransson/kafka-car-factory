package com.factory.batteryproducer.scheduler;

import com.factory.batteryproducer.model.BatteryDataPool;
import com.factory.batteryproducer.model.BatteryLineage;
import com.factory.batteryproducer.producer.BatteryEventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Drives the mock manufacturing simulation.
 *
 * Every tick a weighted topic is chosen and a correlated event is emitted:
 *  - Cell assembly events fire most frequently (many cells per module).
 *  - Formation cycling fires at medium frequency (multiple cycles per cell).
 *  - Module packaging, quality tests and pack dispatches fire rarely
 *    (one per completed assembly).
 */
@Component
public class BatteryProductionScheduler {

    private static final Logger log = LoggerFactory.getLogger(BatteryProductionScheduler.class);

    private final BatteryEventProducer producer;

    // Current lineage – rotated on every new pack
    private BatteryLineage currentLineage = BatteryLineage.random();
    private int cellsInCurrentModule      = 0;
    private int formationCyclesDone       = 0;
    private final int totalFormationCycles = 5;
    private final AtomicBoolean running   = new AtomicBoolean(true);

    @Value("${app.producer.emit-interval-ms:500}")
    private long emitIntervalMs;

    public BatteryProductionScheduler(BatteryEventProducer producer) {
        this.producer = producer;
    }

    /**
     * Main production tick – fires every {@code emit-interval-ms} milliseconds.
     *
     * The simulation walks through the manufacturing stages in a weighted manner
     * that roughly mirrors a real battery gigafactory: lots of cell events, a
     * handful of module events, and one pack-dispatch at the end.
     */
    @Scheduled(fixedDelayString = "${app.producer.emit-interval-ms:500}")
    public void tick() {
        if (!running.get()) return;

        try {
            int roll = BatteryDataPool.randInt(1, 100);

            if (roll <= 45) {
                // --- Cell assembly (most frequent) ---------------------------
                if (cellsInCurrentModule > 0 && BatteryDataPool.chance(0.3)) {
                    currentLineage = currentLineage.nextCell();
                }
                producer.sendCellAssembly(currentLineage);
                cellsInCurrentModule++;

            } else if (roll <= 70) {
                // --- Formation cycling ----------------------------------------
                formationCyclesDone++;
                producer.sendFormationCycling(currentLineage, formationCyclesDone, totalFormationCycles);

            } else if (roll <= 82) {
                // --- Module packaging -----------------------------------------
                producer.sendModulePackaging(currentLineage);

            } else if (roll <= 93) {
                // --- Quality test ---------------------------------------------
                producer.sendQualityTest(currentLineage);

            } else {
                // --- Pack dispatch (least frequent) - rotate lineage after ----
                producer.sendPackDispatch(currentLineage);
                rotateLineage();
            }

        } catch (Exception e) {
            log.error("[battery-producer] Error during tick: {}", e.getMessage(), e);
        }
    }

    /** Rotate to a fresh pack / module / cell after a dispatch. */
    private void rotateLineage() {
        currentLineage      = BatteryLineage.random();
        cellsInCurrentModule = 0;
        formationCyclesDone  = 0;
        log.info("[battery-producer] New production run → pack={} module={} line={}",
                currentLineage.packId(), currentLineage.moduleId(), currentLineage.productionLine());
    }

    public void pause()  { running.set(false); log.info("[battery-producer] Production paused");  }
    public void resume() { running.set(true);  log.info("[battery-producer] Production resumed"); }
    public boolean isRunning() { return running.get(); }
}
