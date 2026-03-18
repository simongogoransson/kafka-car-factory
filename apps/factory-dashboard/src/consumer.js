'use strict';

const { Kafka, logLevel } = require('kafkajs');
const WebSocket = require('ws');

const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const SCHEMA_REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL || 'http://localhost:8081';
const USE_SCHEMA_REGISTRY = process.env.USE_SCHEMA_REGISTRY === 'true';
const WS_PORT  = parseInt(process.env.WS_PORT || '3001', 10);
const GROUP_ID = 'factory-dashboard-consumer';

// Only require Schema Registry if enabled
let SchemaRegistry;
if (USE_SCHEMA_REGISTRY) {
  SchemaRegistry = require('@kafkajs/confluent-schema-registry').SchemaRegistry;
}

const TOPICS = [
  'assembly-line-events',
  'quality-control-results',
  'vehicle-completed',
  'parts-inventory',
  'engine-production',
  'paint-shop-events',
];

// ─── WebSocket server ─────────────────────────────────────────────────────────

const wss = new WebSocket.Server({ port: WS_PORT });

wss.on('listening', () => console.log(`[dashboard] WebSocket server listening on port ${WS_PORT}`));

function broadcast(msg) {
  const data = JSON.stringify(msg);
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  }
}

// ─── Kafka consumer ───────────────────────────────────────────────────────────

async function startConsumer() {
  const kafka = new Kafka({
    clientId: 'factory-dashboard',
    brokers: BROKERS,
    logLevel: logLevel.WARN,
    retry: {
      initialRetryTime: 3000,
      retries: 10,
    },
  });

  let registry;
  if (USE_SCHEMA_REGISTRY) {
    try {
      registry = new SchemaRegistry({ host: SCHEMA_REGISTRY_URL });
      console.log(`[dashboard] Schema Registry enabled at ${SCHEMA_REGISTRY_URL}`);
    } catch (err) {
      console.warn(`[dashboard] Schema Registry unavailable: ${err.message}`);
      console.log('[dashboard] Falling back to plain JSON decoding');
      registry = null;
    }
  } else {
    console.log('[dashboard] Schema Registry disabled - using plain JSON decoding');
  }

  const consumer = kafka.consumer({ groupId: GROUP_ID });

  console.log(`[dashboard] Connecting to Kafka at ${BROKERS.join(', ')}...`);
  await consumer.connect();

  for (const topic of TOPICS) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }

  console.log('[dashboard] Subscribed to all factory topics. Waiting for events...');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      let payload;
      try {
        // Check if this is an Avro message (Confluent wire format starts with magic byte 0x00)
        if (registry && message.value && message.value[0] === 0) {
          payload = await registry.decode(message.value);
        } else {
          payload = JSON.parse(message.value.toString());
        }

        // Unwrap Connect JSON envelopes if present
        if (payload && typeof payload === 'object' && payload.payload && payload.schema) {
          payload = payload.payload;
        }
      } catch {
        payload = { raw: message.value?.toString() || '' };
      }

      const envelope = {
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString(),
        timestamp: message.timestamp,
        payload,
      };

      broadcast(envelope);
    },
  });

  process.on('SIGTERM', async () => {
    console.log('[dashboard] Shutting down consumer...');
    await consumer.disconnect();
    wss.close();
    process.exit(0);
  });
}

module.exports = { startConsumer };
