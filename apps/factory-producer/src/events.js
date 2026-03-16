'use strict';

// ─── Shared data pools ────────────────────────────────────────────────────────

const MODELS = ['Falcon-X', 'Titan-GT', 'Apex-S', 'Orbit-E', 'Meridian-V', 'Zenith-R', 'Nova-EV'];
const COLORS  = ['Midnight Black', 'Alpine White', 'Velocity Red', 'Ocean Blue', 'Granite Grey', 'Solar Yellow', 'Forest Green'];
const ASSEMBLY_STATIONS = [
  'ST-01 Body Framing', 'ST-02 Chassis Mounting', 'ST-03 Engine Install',
  'ST-04 Transmission', 'ST-05 Suspension', 'ST-06 Wiring Harness',
  'ST-07 Interior Trim', 'ST-08 Glass Install', 'ST-09 Doors & Panels',
  'ST-10 Final Inspection',
];
const OPERATIONS = ['weld', 'bolt', 'install', 'calibrate', 'inspect', 'torque', 'seal', 'test'];
const PAINT_STAGES = ['pre-treatment', 'primer', 'base-coat', 'clear-coat', 'quality-check'];
const PART_NAMES = [
  'Front Bumper', 'Rear Axle', 'Brake Caliper', 'Alternator', 'Timing Belt',
  'Fuel Injector', 'Shock Absorber', 'Catalytic Converter', 'Air Filter', 'Wheel Hub',
];
const WAREHOUSES = ['WH-A1', 'WH-A2', 'WH-B1', 'WH-B2', 'WH-C1'];
const INSPECTORS = ['INS-001', 'INS-002', 'INS-003', 'INS-004'];
const QC_CHECKS = ['paint adhesion', 'panel gap', 'door alignment', 'brake test', 'electrical', 'noise/vibration', 'seat belt'];

// ─── VIN generator ────────────────────────────────────────────────────────────

let vinCounter = Math.floor(Math.random() * 50000) + 10000;

function nextVin() {
  const id = String(++vinCounter).padStart(6, '0');
  return `FAC${new Date().getFullYear()}${id}`;
}

function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randFloat(min, max, dp = 1) {
  return parseFloat((Math.random() * (max - min) + min).toFixed(dp));
}

// ─── Event generators ─────────────────────────────────────────────────────────

function assemblyLineEvent() {
  return {
    eventType: 'assembly-line',
    vin: nextVin(),
    model: pick(MODELS),
    station: pick(ASSEMBLY_STATIONS),
    operation: pick(OPERATIONS),
    workerId: `WKR-${randInt(100, 999)}`,
    durationSec: randInt(10, 180),
    torqueNm: randFloat(20, 120),
    status: Math.random() > 0.05 ? 'ok' : 'rework',
    timestamp: new Date().toISOString(),
  };
}

function qualityControlEvent() {
  const passed = Math.random() > 0.08;
  const numChecks = randInt(2, 5);
  const checks = [];
  const pool = [...QC_CHECKS].sort(() => Math.random() - 0.5).slice(0, numChecks);
  pool.forEach((check) => checks.push({ check, result: passed || Math.random() > 0.2 ? 'pass' : 'fail' }));
  return {
    eventType: 'quality-control',
    vin: nextVin(),
    model: pick(MODELS),
    inspector: pick(INSPECTORS),
    station: 'QC-LINE',
    overallResult: passed ? 'pass' : 'fail',
    checks,
    defectCode: passed ? null : `DEF-${randInt(1000, 9999)}`,
    timestamp: new Date().toISOString(),
  };
}

function vehicleCompletedEvent() {
  const productionMinutes = randInt(180, 480);
  return {
    eventType: 'vehicle-completed',
    vin: nextVin(),
    model: pick(MODELS),
    color: pick(COLORS),
    productionTimeMin: productionMinutes,
    productionLine: `LINE-${randInt(1, 4)}`,
    destination: pick(['Export-EU', 'Export-US', 'Domestic-North', 'Domestic-South', 'Dealer-Fleet']),
    deliveryDate: new Date(Date.now() + randInt(7, 30) * 86400000).toISOString().split('T')[0],
    timestamp: new Date().toISOString(),
  };
}

function partsInventoryEvent() {
  const qty = randInt(0, 500);
  let stockStatus;
  if (qty === 0) stockStatus = 'critical';
  else if (qty < 50) stockStatus = 'low';
  else stockStatus = 'ok';

  return {
    eventType: 'parts-inventory',
    partName: pick(PART_NAMES),
    partNumber: `P-${randInt(10000, 99999)}`,
    quantity: qty,
    reorderPoint: 50,
    stockStatus,
    warehouse: pick(WAREHOUSES),
    supplier: `SUPP-${randInt(1, 20)}`,
    timestamp: new Date().toISOString(),
  };
}

function engineProductionEvent() {
  const cylinders = pick([4, 6, 8]);
  return {
    eventType: 'engine-production',
    engineSerial: `ENG-${randInt(100000, 999999)}`,
    model: pick(MODELS),
    type: pick(['V' + cylinders, 'Inline-' + cylinders, 'Electric', 'Hybrid']),
    displacementCC: cylinders === 4 ? randInt(1400, 2000) : cylinders === 6 ? randInt(2500, 3500) : randInt(4000, 6200),
    cylinders,
    torqueNm: randInt(150, 600),
    powerKw: randInt(80, 450),
    assemblyTimeMin: randInt(30, 90),
    testResult: Math.random() > 0.04 ? 'pass' : 'fail',
    assemblyLine: `ENG-LINE-${randInt(1, 3)}`,
    timestamp: new Date().toISOString(),
  };
}

function paintShopEvent() {
  return {
    eventType: 'paint-shop',
    vin: nextVin(),
    model: pick(MODELS),
    color: pick(COLORS),
    colorCode: `COL-${randInt(100, 999)}`,
    boothNumber: randInt(1, 8),
    stage: pick(PAINT_STAGES),
    temperatureC: randFloat(20, 80),
    humidityPct: randFloat(40, 70),
    durationMin: randInt(15, 60),
    coatThicknessMicron: randFloat(60, 120),
    status: Math.random() > 0.03 ? 'ok' : 'defect',
    timestamp: new Date().toISOString(),
  };
}

// ─── Dispatch ─────────────────────────────────────────────────────────────────

const GENERATORS = {
  'assembly-line-events': assemblyLineEvent,
  'quality-control-results': qualityControlEvent,
  'vehicle-completed': vehicleCompletedEvent,
  'parts-inventory': partsInventoryEvent,
  'engine-production': engineProductionEvent,
  'paint-shop-events': paintShopEvent,
};

function generateEvent(topic) {
  const gen = GENERATORS[topic];
  if (!gen) throw new Error(`Unknown topic: ${topic}`);
  return gen();
}

module.exports = { generateEvent };
