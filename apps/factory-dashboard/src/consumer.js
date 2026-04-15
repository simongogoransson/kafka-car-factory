'use strict';

const { Kafka, logLevel, CompressionTypes, CompressionCodecs } = require('kafkajs');
const SnappyCodec = require('kafkajs-snappy');
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

const WebSocket = require('ws');
const avsc = require('avsc');

const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const APICURIO_REGISTRY_URL = process.env.APICURIO_REGISTRY_URL || 'http://localhost:8080';
const USE_APICURIO_REGISTRY = process.env.USE_APICURIO_REGISTRY === 'true';
const WS_PORT  = parseInt(process.env.WS_PORT || '3001', 10);
const GROUP_ID = 'factory-dashboard-consumer';

const TOPICS = [
  'assembly-line-events',
  'quality-control-results',
  'vehicle-completed',
  'parts-inventory',
  'engine-production',
  'paint-shop-events',
];

const BATTERY_TOPICS = new Set([
  'battery-cell-assembly',
  'battery-module-packaging',
  'battery-formation-cycling',
  'battery-quality-test',
  'battery-pack-dispatch',
]);

// --- Avro deserialization ----------------------------------------------------
// Apicurio wire format: 0x00 (magic) + 8-byte big-endian globalId + Avro bytes

const schemaCache = new Map();

async function deserializeAvro(buffer) {
  const globalId = buffer.readUInt32BE(5); // low 4 bytes of 8-byte globalId
  let type = schemaCache.get(globalId);
  if (!type) {
    const url = `${APICURIO_REGISTRY_URL}/apis/registry/v2/ids/globalIds/${globalId}`;
    const res = await fetch(url, { headers: { Accept: 'application/json' } });
    if (!res.ok) throw new Error(`Failed to fetch schema for globalId ${globalId}: ${res.status}`);
    type = avsc.Type.forSchema(await res.json());
    schemaCache.set(globalId, type);
  }
  return type.fromBuffer(buffer.subarray(9));
}

// --- WebSocket server --------------------------------------------------------

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

// --- Kafka consumer ----------------------------------------------------------

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

  if (USE_APICURIO_REGISTRY) {
    console.log(`[dashboard] Apicurio Registry configured at ${APICURIO_REGISTRY_URL}`);
  } else {
    console.log('[dashboard] Apicurio Registry disabled - using plain JSON decoding');
  }

  const consumer = kafka.consumer({ groupId: GROUP_ID });

  console.log(`[dashboard] Connecting to Kafka at ${BROKERS.join(', ')}...`);
  await consumer.connect();

  for (const topic of TOPICS) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }
  for (const topic of BATTERY_TOPICS) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }

  console.log('[dashboard] Subscribed to all factory topics. Waiting for events...');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      let payload;
      try {
        const raw = message.value;
        if (USE_APICURIO_REGISTRY && raw && raw[0] === 0x00) {
          payload = await deserializeAvro(raw);
        } else {
          payload = JSON.parse(raw.toString());
          // Unwrap Connect JSON envelopes if present
          if (payload && typeof payload === 'object' && payload.payload && payload.schema) {
            payload = payload.payload;
          }
        }
      } catch {
        payload = { raw: message.value?.toString() || '' };
      }

      const envelope = {
        topic,
        channel: BATTERY_TOPICS.has(topic) ? 'battery' : 'factory',
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
