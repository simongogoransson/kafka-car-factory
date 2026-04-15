package com.factory.batteryproducer.producer;

import com.factory.battery.events.*;
import com.factory.batteryproducer.model.BatteryDataPool;
import com.factory.batteryproducer.model.BatteryLineage;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * Generates realistic mock Avro events for every stage of the
 * car-battery manufacturing process.
 */
@Component
public class BatteryEventFactory {

    // --- Cell Assembly -------------------------------------------------------

    public CellAssemblyEvent cellAssemblyEvent(BatteryLineage lineage) {
        boolean ok = BatteryDataPool.chance(0.94);
        return CellAssemblyEvent.newBuilder()
                .setEventId(uuid())
                .setEventType("cell-assembly")
                .setCellId(lineage.cellId())
                .setModuleId(lineage.moduleId())
                .setChemistry(lineage.chemistry())
                .setStation(BatteryDataPool.pick(BatteryDataPool.CELL_STATIONS))
                .setOperation(BatteryDataPool.pick(BatteryDataPool.CELL_OPERATIONS))
                .setWorkerId("WKR-" + BatteryDataPool.randInt(100, 999))
                .setCapacityMah(BatteryDataPool.randFloat(3_000, 5_500, 1))
                .setVoltageV(BatteryDataPool.randFloat(3.50, 4.20, 3))
                .setThicknessMm(BatteryDataPool.randFloat(5.0, 12.0, 2))
                .setWeightG(BatteryDataPool.randFloat(45.0, 120.0, 1))
                .setStatus(ok ? "ok" : (BatteryDataPool.chance(0.6) ? "rework" : "scrap"))
                .setDefectCode(ok ? null : BatteryDataPool.pick(BatteryDataPool.DEFECT_CODES))
                .setProductionLine(lineage.productionLine())
                .setTimestamp(now())
                .build();
    }

    // --- Module Packaging ----------------------------------------------------

    public ModulePackagingEvent modulePackagingEvent(BatteryLineage lineage) {
        String config = BatteryDataPool.pick(BatteryDataPool.CONFIGURATIONS);
        int cells = parseCellCount(config);
        float nomV  = BatteryDataPool.randFloat(48.0, 400.0, 1);
        float capKwh = BatteryDataPool.randFloat(5.0, 30.0, 2);
        boolean ok  = BatteryDataPool.chance(0.95);
        return ModulePackagingEvent.newBuilder()
                .setEventId(uuid())
                .setEventType("module-packaging")
                .setModuleId(lineage.moduleId())
                .setPackId(lineage.packId())
                .setChemistry(lineage.chemistry())
                .setCellCount(cells)
                .setConfiguration(config)
                .setNominalVoltageV(nomV)
                .setCapacityKwh(capKwh)
                .setMassKg(BatteryDataPool.randFloat(8.0, 40.0, 1))
                .setInternalResistanceMohm(BatteryDataPool.randFloat(0.5, 8.0, 2))
                .setCoolingType(BatteryDataPool.pick(BatteryDataPool.COOLING_TYPES))
                .setStation(BatteryDataPool.pick(BatteryDataPool.MODULE_STATIONS))
                .setStatus(ok ? "ok" : "rework")
                .setProductionLine(lineage.productionLine())
                .setTimestamp(now())
                .build();
    }

    // --- Formation Cycling ---------------------------------------------------

    public FormationCyclingEvent formationCyclingEvent(BatteryLineage lineage, int cycle, int totalCycles) {
        float chargeCapacity    = BatteryDataPool.randFloat(3_800, 5_400, 1);
        float dischargeCapacity = chargeCapacity * BatteryDataPool.randFloat(0.96, 0.999, 4);
        float efficiency        = (dischargeCapacity / chargeCapacity) * 100f;
        boolean ok              = efficiency > 95.0f;
        return FormationCyclingEvent.newBuilder()
                .setEventId(uuid())
                .setEventType("formation-cycling")
                .setCellId(lineage.cellId())
                .setCycleNumber(cycle)
                .setTotalCycles(totalCycles)
                .setCycleType(cycle % 2 == 0 ? "charge" : "discharge")
                .setCRate(BatteryDataPool.pick(java.util.List.of(0.1f, 0.2f, 0.5f, 1.0f)))
                .setStartVoltageV(BatteryDataPool.randFloat(2.8, 3.2, 3))
                .setEndVoltageV(BatteryDataPool.randFloat(4.1, 4.25, 3))
                .setChargeCapacityMah(chargeCapacity)
                .setDischargeCapacityMah(dischargeCapacity)
                .setCoulombicEfficiencyPct((float) Math.round(efficiency * 100) / 100f)
                .setTemperatureC(BatteryDataPool.randFloat(22.0, 45.0, 1))
                .setDurationMin(BatteryDataPool.randInt(60, 300))
                .setChamberId("CHAMBER-" + BatteryDataPool.randInt(1, 16))
                .setStatus(ok ? "ok" : "fail")
                .setTimestamp(now())
                .build();
    }

    // --- Quality Test --------------------------------------------------------

    public QualityTestEvent qualityTestEvent(BatteryLineage lineage) {
        boolean pass = BatteryDataPool.chance(0.92);
        String grade = pass ? BatteryDataPool.pick(BatteryDataPool.GRADES) : "scrap";
        return QualityTestEvent.newBuilder()
                .setEventId(uuid())
                .setEventType("quality-test")
                .setPackId(lineage.packId())
                .setInspector(BatteryDataPool.pick(BatteryDataPool.INSPECTORS))
                .setTestType(BatteryDataPool.pick(java.util.List.of("EOL", "incoming", "periodic")))
                .setCapacityRetentionPct(BatteryDataPool.randFloat(88.0, 101.0, 2))
                .setInternalResistanceMohm(BatteryDataPool.randFloat(2.0, 25.0, 2))
                .setShortCircuitTest(pass || BatteryDataPool.chance(0.5))
                .setInsulationResistanceMohm(BatteryDataPool.randFloat(500, 10_000, 0))
                .setThermalRunawayTest(pass || BatteryDataPool.chance(0.7))
                .setOverallResult(pass ? "pass" : "fail")
                .setDefectCode(pass ? null : BatteryDataPool.pick(BatteryDataPool.DEFECT_CODES))
                .setGradeAssigned(grade)
                .setStation("EOL-QC-" + BatteryDataPool.randInt(1, 4))
                .setTimestamp(now())
                .build();
    }

    // --- Pack Dispatch -------------------------------------------------------

    public PackDispatchEvent packDispatchEvent(BatteryLineage lineage) {
        float capKwh  = BatteryDataPool.randFloat(40.0, 120.0, 2);
        float massKg  = BatteryDataPool.randFloat(200.0, 650.0, 1);
        float nomV    = BatteryDataPool.randFloat(300.0, 800.0, 1);
        float density = (capKwh * 1000f) / massKg;
        int   mfgTime = BatteryDataPool.randInt(120, 480);
        String dispatchDate = LocalDate.now()
                .plusDays(BatteryDataPool.randInt(3, 21))
                .format(DateTimeFormatter.ISO_LOCAL_DATE);
        return PackDispatchEvent.newBuilder()
                .setEventId(uuid())
                .setEventType("pack-dispatch")
                .setPackId(lineage.packId())
                .setChemistry(lineage.chemistry())
                .setCapacityKwh(capKwh)
                .setNominalVoltageV(nomV)
                .setEnergyDensityWhKg((float) Math.round(density * 10) / 10f)
                .setMassKg(massKg)
                .setGrade(BatteryDataPool.pick(BatteryDataPool.GRADES))
                .setDestinationOem(lineage.oemCode())
                .setVehicleModel(lineage.vehicleModel())
                .setWarrantyYears(BatteryDataPool.pick(java.util.List.of(5, 8, 10)))
                .setManufacturingTimeMin(mfgTime)
                .setDispatchDate(dispatchDate)
                .setProductionLine(lineage.productionLine())
                .setTimestamp(now())
                .build();
    }

    // --- Helpers -------------------------------------------------------------

    private static String uuid() {
        return UUID.randomUUID().toString();
    }

    private static String now() {
        return Instant.now().toString();
    }

    /** Parse cell count from config string like "12s4p" → 12*4 = 48. */
    private int parseCellCount(String config) {
        try {
            String[] parts = config.toLowerCase().split("s");
            int series  = Integer.parseInt(parts[0]);
            int parallel = Integer.parseInt(parts[1].replace("p", ""));
            return series * parallel;
        } catch (Exception e) {
            return BatteryDataPool.randInt(24, 96);
        }
    }
}
