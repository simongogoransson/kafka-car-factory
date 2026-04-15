package com.factory.batteryproducer.producer;

import com.factory.batteryproducer.model.BatteryLineage;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Sends battery manufacturing events to the appropriate Kafka topics.
 */
@Service
public class BatteryEventProducer {

    private static final Logger log = LoggerFactory.getLogger(BatteryEventProducer.class);

    public static final String TOPIC_CELL_ASSEMBLY    = "battery-cell-assembly";
    public static final String TOPIC_MODULE_PACKAGING = "battery-module-packaging";
    public static final String TOPIC_FORMATION        = "battery-formation-cycling";
    public static final String TOPIC_QUALITY_TEST     = "battery-quality-test";
    public static final String TOPIC_PACK_DISPATCH    = "battery-pack-dispatch";

    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;
    private final BatteryEventFactory eventFactory;

    // Stats
    private volatile long totalEventsSent = 0;

    public BatteryEventProducer(KafkaTemplate<String, SpecificRecord> kafkaTemplate,
                                BatteryEventFactory eventFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventFactory  = eventFactory;
    }

    // --- Public send methods -------------------------------------------------

    public void sendCellAssembly(BatteryLineage lineage) {
        send(TOPIC_CELL_ASSEMBLY, lineage.cellId(), eventFactory.cellAssemblyEvent(lineage));
    }

    public void sendModulePackaging(BatteryLineage lineage) {
        send(TOPIC_MODULE_PACKAGING, lineage.moduleId(), eventFactory.modulePackagingEvent(lineage));
    }

    public void sendFormationCycling(BatteryLineage lineage, int cycle, int totalCycles) {
        send(TOPIC_FORMATION, lineage.cellId(), eventFactory.formationCyclingEvent(lineage, cycle, totalCycles));
    }

    public void sendQualityTest(BatteryLineage lineage) {
        send(TOPIC_QUALITY_TEST, lineage.packId(), eventFactory.qualityTestEvent(lineage));
    }

    public void sendPackDispatch(BatteryLineage lineage) {
        send(TOPIC_PACK_DISPATCH, lineage.packId(), eventFactory.packDispatchEvent(lineage));
    }

    public long getTotalEventsSent() {
        return totalEventsSent;
    }

    // --- Internal ------------------------------------------------------------

    private void send(String topic, String key, SpecificRecord record) {
        CompletableFuture<SendResult<String, SpecificRecord>> future =
                kafkaTemplate.send(topic, key, record);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[battery-producer] Failed to send to {}: {}", topic, ex.getMessage());
            } else {
                totalEventsSent++;
                log.debug("[battery-producer] Sent {} → {} (partition={}, offset={})",
                        record.getSchema().getName(), topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
