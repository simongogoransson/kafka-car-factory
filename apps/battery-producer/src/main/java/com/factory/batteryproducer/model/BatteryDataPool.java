package com.factory.batteryproducer.model;

import java.util.List;
import java.util.Random;

/**
 * Shared data pools and helper utilities for generating realistic
 * car-battery manufacturing mock data.
 */
public final class BatteryDataPool {

    private BatteryDataPool() {}

    public static final List<String> CHEMISTRIES      = List.of("NMC811", "LFP", "NCA", "LMFP");
    public static final List<String> PRODUCTION_LINES = List.of("BATT-LINE-1", "BATT-LINE-2", "BATT-LINE-3");
    public static final List<String> COOLING_TYPES    = List.of("liquid", "air", "phase-change");
    public static final List<String> VEHICLE_MODELS   = List.of(
            "Apex-S EV", "Orbit-E Pro", "Meridian-V EV", "Nova-EV Sport", "Zenith-R Electric"
    );
    public static final List<String> OEM_CODES        = List.of(
            "OEM-Apex", "OEM-Orbit", "OEM-Meridian", "OEM-Nova", "OEM-Zenith", "OEM-Fleet"
    );
    public static final List<String> INSPECTORS       = List.of("INS-B01", "INS-B02", "INS-B03", "INS-B04");
    public static final List<String> GRADES           = List.of("A", "A", "A", "B", "B", "C");  // weighted

    public static final List<String> CELL_OPERATIONS  = List.of(
            "electrode-stacking", "electrolyte-fill", "tab-welding",
            "seal-crimp", "formation-charge", "leak-test", "capacity-check"
    );
    public static final List<String> CELL_STATIONS = List.of(
            "CS-01 Electrode Prep", "CS-02 Winding", "CS-03 Stacking",
            "CS-04 Electrolyte Fill", "CS-05 Sealing", "CS-06 Formation",
            "CS-07 Ageing", "CS-08 Final Grade"
    );
    public static final List<String> MODULE_STATIONS  = List.of(
            "MS-01 Cell Array", "MS-02 Busbar Weld", "MS-03 BMS Install",
            "MS-04 Thermal Pad", "MS-05 Enclosure", "MS-06 Module Test"
    );
    public static final List<String> CONFIGURATIONS   = List.of(
            "12s4p", "8s6p", "16s3p", "24s2p", "6s8p"
    );

    public static final List<String> DEFECT_CODES = List.of(
            "DEF-B001 LowCapacity", "DEF-B002 HighResistance", "DEF-B003 Leak",
            "DEF-B004 ShortCircuit", "DEF-B005 ThermalAnomaly", "DEF-B006 TabWeld"
    );

    // --- Counters ------------------------------------------------------------

    private static long cellCounter   = (long) (Math.random() * 200_000) + 100_000;
    private static long moduleCounter = (long) (Math.random() *  50_000) +  10_000;
    private static long packCounter   = (long) (Math.random() *  20_000) +   5_000;

    public static synchronized String nextCellId() {
        return String.format("CELL-%07d", ++cellCounter);
    }

    public static synchronized String nextModuleId() {
        return String.format("MOD-%06d", ++moduleCounter);
    }

    public static synchronized String nextPackId() {
        return String.format("PACK-%05d", ++packCounter);
    }

    // --- Random helpers ------------------------------------------------------

    private static final Random RNG = new Random();

    public static <T> T pick(List<T> list) {
        return list.get(RNG.nextInt(list.size()));
    }

    public static int randInt(int min, int max) {
        return RNG.nextInt(max - min + 1) + min;
    }

    public static float randFloat(double min, double max) {
        return (float) (RNG.nextDouble() * (max - min) + min);
    }

    public static float randFloat(double min, double max, int dp) {
        double v = RNG.nextDouble() * (max - min) + min;
        double scale = Math.pow(10, dp);
        return (float) (Math.round(v * scale) / scale);
    }

    public static boolean chance(double probability) {
        return RNG.nextDouble() < probability;
    }
}

