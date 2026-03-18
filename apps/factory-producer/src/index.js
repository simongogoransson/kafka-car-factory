'use strict';

const { Kafka, logLevel } = require('kafkajs');
const { generateEvent } = require('./events');

const BROKERS          = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const SCHEMA_REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL || 'http://localhost:8081';
const USE_SCHEMA_REGISTRY = process.env.USE_SCHEMA_REGISTRY === 'true';
const CLIENT_ID        = 'factory-producer';

// Only require Schema Registry if enabled
let SchemaRegistry, SchemaType;
if (USE_SCHEMA_REGISTRY) {
  const schemaRegistryModule = require('@kafkajs/confluent-schema-registry');
  SchemaRegistry = schemaRegistryModule.SchemaRegistry;
  SchemaType = schemaRegistryModule.SchemaType;
}

const TOPICS = [
  'assembly-line-events',
  'quality-control-results',
  'vehicle-completed',
  'parts-inventory',
  'engine-production',
  'paint-shop-events',
];

// Weighted topic distribution: assembly line fires most, vehicle-completed least
const TOPIC_WEIGHTS = [35, 20, 5, 15, 15, 10];
const TOTAL_WEIGHT = TOPIC_WEIGHTS.reduce((a, b) => a + b, 0);

function pickTopic() {
  let r = Math.random() * TOTAL_WEIGHT;
  for (let i = 0; i < TOPICS.length; i++) {
    r -= TOPIC_WEIGHTS[i];
    if (r <= 0) return TOPICS[i];
  }
  return TOPICS[0];
}

// ─── Avro schema for vehicle-completed ───────────────────────────────────────

const VEHICLE_COMPLETED_AVRO_SCHEMA = {
  type: 'record',
  name: 'VehicleCompleted',
  namespace: 'factory',
  fields: [
    { name: 'eventType',         type: 'string' },
    { name: 'vin',               type: 'string' },
    { name: 'model',             type: 'string' },
    { name: 'color',             type: 'string' },
    { name: 'productionTimeMin', type: 'int'    },
    { name: 'productionLine',    type: 'string' },
    { name: 'destination',       type: 'string' },
    { name: 'deliveryDate',      type: 'string' },
    { name: 'timestamp',         type: 'string' },
  ],
};

async function run() {
  const kafka = new Kafka({
    clientId: CLIENT_ID,
    brokers: BROKERS,
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 3000, retries: 10 },
  });

  let registry, schemaId;
  if (USE_SCHEMA_REGISTRY) {
    try {
      registry = new SchemaRegistry({ host: SCHEMA_REGISTRY_URL });
      // Register schema once — returns existing id if already registered
      const result = await registry.register(
        { type: SchemaType.AVRO, schema: JSON.stringify(VEHICLE_COMPLETED_AVRO_SCHEMA) },
        { subject: 'vehicle-completed-value' }
      );
      schemaId = result.id;
      console.log(`[factory-producer] vehicle-completed Avro schema registered (id: ${schemaId})`);
      console.log(`[factory-producer] Schema Registry enabled at ${SCHEMA_REGISTRY_URL}`);
    } catch (err) {
      console.warn(`[factory-producer] Schema Registry unavailable: ${err.message}`);
      console.log('[factory-producer] Falling back to plain JSON encoding');
      registry = null;
    }
  } else {
    console.log('[factory-producer] Schema Registry disabled - using plain JSON encoding');
  }
  const producer = kafka.producer();
  console.log(`[factory-producer] Connecting to Kafka at ${BROKERS.join(', ')}...`);
  await producer.connect();
  console.log('[factory-producer] Connected. Starting event generation...');

  let eventCount = 0;

  process.on('SIGTERM', async () => {
    console.log('[factory-producer] Shutting down...');
    await producer.disconnect();
    process.exit(0);
  });

  while (true) {
    const topic = pickTopic();
    const event = generateEvent(topic);

    let value;
    if (topic === 'vehicle-completed' && registry && schemaId) {
      // Encode as Avro with schema id prefix (Confluent wire format)
      value = await registry.encode(schemaId, event);
    } else {
      // Plain JSON encoding
      value = Buffer.from(JSON.stringify(event));
    }

    await producer.send({
      topic,
      messages: [{
        key: event.vin || event.partNumber || event.engineSerial || String(Date.now()),
        value,
        timestamp: String(Date.now()),
      }],
    });

    eventCount++;
    if (eventCount % 10 === 0) {
      console.log(`[factory-producer] ${eventCount} events sent. Last: [${topic}] ${event.vin || event.partNumber || ''}`);
    }

    const delay = 300 + Math.floor(Math.random() * 2200);
    await new Promise((r) => setTimeout(r, delay));
  }
}

run().catch((err) => {
  console.error('[factory-producer] Fatal error:', err);
  process.exit(1);
});
