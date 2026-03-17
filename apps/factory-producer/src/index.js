'use strict';

const { Kafka, logLevel } = require('kafkajs');
const { generateEvent } = require('./events');

const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const CLIENT_ID = 'factory-producer';

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

const VEHICLE_COMPLETED_CONNECT_SCHEMA = {
  type: 'struct',
  name: 'factory.vehicle_completed.v1',
  optional: false,
  fields: [
    { field: 'eventType', type: 'string', optional: false },
    { field: 'vin', type: 'string', optional: false },
    { field: 'model', type: 'string', optional: false },
    { field: 'color', type: 'string', optional: false },
    { field: 'productionTimeMin', type: 'int32', optional: false },
    { field: 'productionLine', type: 'string', optional: false },
    { field: 'destination', type: 'string', optional: false },
    { field: 'deliveryDate', type: 'string', optional: false },
    { field: 'timestamp', type: 'string', optional: false },
  ],
};

function serializeForTopic(topic, event) {
  if (topic !== 'vehicle-completed') return JSON.stringify(event);

  return JSON.stringify({
    schema: VEHICLE_COMPLETED_CONNECT_SCHEMA,
    payload: event,
  });
}

async function run() {
  const kafka = new Kafka({
    clientId: CLIENT_ID,
    brokers: BROKERS,
    logLevel: logLevel.WARN,
    retry: {
      initialRetryTime: 3000,
      retries: 10,
    },
  });

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

    await producer.send({
      topic,
      messages: [
        {
          key: event.vin || event.partNumber || event.engineSerial || String(event.id || Date.now()),
          value: serializeForTopic(topic, event),
          timestamp: String(Date.now()),
        },
      ],
    });

    eventCount++;
    if (eventCount % 10 === 0) {
      console.log(`[factory-producer] ${eventCount} events sent. Last: [${topic}] ${event.vin || event.partNumber || ''}`);
    }

    // Random interval: 300ms – 2500ms
    const delay = 300 + Math.floor(Math.random() * 2200);
    await new Promise((r) => setTimeout(r, delay));
  }
}

run().catch((err) => {
  console.error('[factory-producer] Fatal error:', err);
  process.exit(1);
});
