package com.factory.batteryproducer.model;

import java.util.List;

/**
 * Holds state that is shared across correlated events (cell → module → pack).
 * A lineage object is created once and reused so that cell/module/pack IDs
 * match throughout the mock production run.
 */
public record BatteryLineage(
        String packId,
        String moduleId,
        String cellId,
        String chemistry,
        String productionLine,
        String vehicleModel,
        String oemCode
) {
    public static BatteryLineage random() {
        return new BatteryLineage(
                BatteryDataPool.nextPackId(),
                BatteryDataPool.nextModuleId(),
                BatteryDataPool.nextCellId(),
                BatteryDataPool.pick(BatteryDataPool.CHEMISTRIES),
                BatteryDataPool.pick(BatteryDataPool.PRODUCTION_LINES),
                BatteryDataPool.pick(BatteryDataPool.VEHICLE_MODELS),
                BatteryDataPool.pick(BatteryDataPool.OEM_CODES)
        );
    }

    /** Extra cell within the same module (different cell, same module/pack). */
    public BatteryLineage nextCell() {
        return new BatteryLineage(
                packId, moduleId,
                BatteryDataPool.nextCellId(),
                chemistry, productionLine, vehicleModel, oemCode
        );
    }

    public static List<String> chemistries() {
        return BatteryDataPool.CHEMISTRIES;
    }
}

