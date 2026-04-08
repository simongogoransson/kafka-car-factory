'use strict';

const http = require('http');
const { Kafka, logLevel } = require('kafkajs');
const avsc = require('avsc');
const { generateEvent } = require('./events');

const BROKERS          = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const APICURIO_REGISTRY_URL = process.env.APICURIO_REGISTRY_URL || 'http://localhost:8080';
const USE_APICURIO_REGISTRY = process.env.USE_APICURIO_REGISTRY === 'true';
const CLIENT_ID        = 'factory-producer';
const CONTROL_PORT     = parseInt(process.env.CONTROL_PORT || '3002', 10);
const APICURIO_RETRY_MS = parseInt(process.env.APICURIO_RETRY_MS || '15000', 10);

const LOAD_PROFILES = {
  normal: { minDelayMs: 300, maxDelayMs: 2500, label: 'Normal load' },
  heavy: { minDelayMs: 25, maxDelayMs: 120, label: 'Heavy load' },
};

let currentLoadProfile = LOAD_PROFILES.heavy && process.env.LOAD_PROFILE === 'heavy' ? 'heavy' : 'normal';
let totalEventsSent = 0;
let loadProfileChanges = 0;

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

// ─── Kafka Connect JSON schema envelope for vehicle-completed ────────────────
// The Debezium JDBC sink connector requires messages with an embedded schema.
// We wrap vehicle-completed events in the Kafka Connect JSON schema envelope.

const VEHICLE_COMPLETED_SCHEMA_ENVELOPE = {
  schema: {
    type: 'struct',
    fields: [
      { field: 'eventType',         type: 'string'  },
      { field: 'vin',               type: 'string'  },
      { field: 'model',             type: 'string'  },
      { field: 'color',             type: 'string'  },
      { field: 'productionTimeMin', type: 'int32'   },
      { field: 'productionLine',    type: 'string'  },
      { field: 'destination',       type: 'string'  },
      { field: 'deliveryDate',      type: 'string'  },
      { field: 'timestamp',         type: 'string'  },
    ],
    optional: false,
    name: 'VehicleCompleted',
  },
};

const VEHICLE_COMPLETED_AVRO_SCHEMA = {
  type: 'record',
  name: 'VehicleCompleted',
  namespace: 'com.factory.events',
  fields: [
    { name: 'eventType', type: 'string' },
    { name: 'vin', type: 'string' },
    { name: 'model', type: 'string' },
    { name: 'color', type: 'string' },
    { name: 'productionTimeMin', type: 'int' },
    { name: 'productionLine', type: 'string' },
    { name: 'destination', type: 'string' },
    { name: 'deliveryDate', type: 'string' },
    { name: 'timestamp', type: 'string' },
  ],
};

const vehicleCompletedAvroType = avsc.Type.forSchema(VEHICLE_COMPLETED_AVRO_SCHEMA);
let avroGlobalId = null;
let apicurioRegistrationInFlight = false;
let lastApicurioErrorLogTs = 0;

// Apicurio wire format: magic byte (0x00) + 8-byte big-endian globalId + Avro bytes
function serializeAvro(payload, globalId) {
  const avroBytes = vehicleCompletedAvroType.toBuffer(payload);
  const header = Buffer.alloc(9);
  header[0] = 0x00;
  header.writeUInt32BE(0, 1);         // high 4 bytes of 8-byte globalId (always 0)
  header.writeUInt32BE(globalId, 5);  // low 4 bytes of 8-byte globalId
  return Buffer.concat([header, avroBytes]);
}

function wrapWithSchema(payload) {
  return { ...VEHICLE_COMPLETED_SCHEMA_ENVELOPE, payload };
}

async function registerApicurioArtifact() {
  const artifactId = 'vehicle-completed-value';
  const base = `${APICURIO_REGISTRY_URL}/apis/registry/v2`;

  const metaResponse = await fetch(`${base}/groups/default/artifacts/${artifactId}/meta`, { method: 'GET' });
  if (metaResponse.ok) {
    const meta = await metaResponse.json();
    console.log(`[factory-producer] Apicurio artifact already exists: ${artifactId} (globalId=${meta.globalId})`);
    return meta.globalId;
  }

  if (metaResponse.status !== 404) {
    throw new Error(`unexpected status from Apicurio meta endpoint: ${metaResponse.status}`);
  }

  const createResponse = await fetch(`${base}/groups/default/artifacts`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Registry-ArtifactId': artifactId,
      'X-Registry-ArtifactType': 'AVRO',
    },
    body: JSON.stringify(VEHICLE_COMPLETED_AVRO_SCHEMA),
  });

  if (!createResponse.ok) {
    throw new Error(`failed to create Apicurio artifact ${artifactId}: ${createResponse.status}`);
  }

  const created = await createResponse.json();
  console.log(`[factory-producer] Registered Apicurio artifact: ${artifactId} (globalId=${created.globalId})`);
  return created.globalId;
}

async function ensureApicurioRegistration() {
  if (!USE_APICURIO_REGISTRY || avroGlobalId !== null || apicurioRegistrationInFlight) {
    return;
  }

  apicurioRegistrationInFlight = true;
  try {
    avroGlobalId = await registerApicurioArtifact();
    console.log(`[factory-producer] Apicurio Registry enabled at ${APICURIO_REGISTRY_URL} - Avro serialization active`);
  } catch (err) {
    // Throttle repeated startup-race warnings while registry is still coming up.
    const now = Date.now();
    if (now - lastApicurioErrorLogTs >= 30000) {
      console.warn(`[factory-producer] Apicurio Registry unavailable: ${err.message}`);
      console.log('[factory-producer] Continuing with JSON payloads until registry becomes available');
      lastApicurioErrorLogTs = now;
    }
  } finally {
    apicurioRegistrationInFlight = false;
  }
}

function getLoadState() {
  return {
    profile: currentLoadProfile,
    label: LOAD_PROFILES[currentLoadProfile].label,
    delay: LOAD_PROFILES[currentLoadProfile],
    eventsTotal: totalEventsSent,
  };
}

function renderMetrics() {
  const lines = [
    '# HELP factory_producer_load_profile Current producer load profile as a labeled gauge.',
    '# TYPE factory_producer_load_profile gauge',
    `factory_producer_load_profile{profile="normal"} ${currentLoadProfile === 'normal' ? 1 : 0}`,
    `factory_producer_load_profile{profile="heavy"} ${currentLoadProfile === 'heavy' ? 1 : 0}`,
    '# HELP factory_producer_events_sent_total Total number of events sent by the producer.',
    '# TYPE factory_producer_events_sent_total counter',
    `factory_producer_events_sent_total ${totalEventsSent}`,
    '# HELP factory_producer_load_profile_changes_total Total number of producer load profile changes.',
    '# TYPE factory_producer_load_profile_changes_total counter',
    `factory_producer_load_profile_changes_total ${loadProfileChanges}`,
    '',
  ];

  return lines.join('\n');
}

function setLoadProfile(profile) {
  if (!LOAD_PROFILES[profile]) {
    return false;
  }

  if (currentLoadProfile !== profile) {
    loadProfileChanges++;
  }

  currentLoadProfile = profile;
  console.log(`[factory-producer] Load profile changed to ${LOAD_PROFILES[profile].label.toLowerCase()}`);
  return true;
}

function startControlServer() {
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

    if (req.method === 'GET' && url.pathname === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', ...getLoadState() }));
      return;
    }

    if (req.method === 'GET' && url.pathname === '/control/state') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(getLoadState()));
      return;
    }

    if (req.method === 'GET' && url.pathname === '/metrics') {
      res.writeHead(200, { 'Content-Type': 'text/plain; version=0.0.4; charset=utf-8' });
      res.end(renderMetrics());
      return;
    }

    if (req.method === 'POST' && url.pathname === '/control/load-profile') {
      let body = '';
      req.on('data', (chunk) => {
        body += chunk;
      });
      req.on('end', () => {
        try {
          const payload = body ? JSON.parse(body) : {};
          if (!setLoadProfile(payload.profile)) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Unsupported profile', supportedProfiles: Object.keys(LOAD_PROFILES) }));
            return;
          }

          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(getLoadState()));
        } catch (error) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Invalid JSON body' }));
        }
      });
      return;
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not found' }));
  });

  server.listen(CONTROL_PORT, () => {
    console.log(`[factory-producer] Control server listening on port ${CONTROL_PORT}`);
  });

  return server;
}

async function run() {
  const kafka = new Kafka({
    clientId: CLIENT_ID,
    brokers: BROKERS,
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 3000, retries: 10 },
  });

  if (USE_APICURIO_REGISTRY) {
    await ensureApicurioRegistration();
  } else {
    console.log('[factory-producer] Apicurio Registry disabled - using plain JSON encoding');
  }

  const apicurioRetryTimer = USE_APICURIO_REGISTRY
    ? setInterval(() => {
      ensureApicurioRegistration().catch(() => {});
    }, APICURIO_RETRY_MS)
    : null;

  const producer = kafka.producer();
  const controlServer = startControlServer();
  console.log(`[factory-producer] Connecting to Kafka at ${BROKERS.join(', ')}...`);
  await producer.connect();
  console.log('[factory-producer] Connected. Starting event generation...');

  let eventCount = 0;

  process.on('SIGTERM', async () => {
    console.log('[factory-producer] Shutting down...');
    if (apicurioRetryTimer) {
      clearInterval(apicurioRetryTimer);
    }
    controlServer.close();
    await producer.disconnect();
    process.exit(0);
  });

  while (true) {
    const topic = pickTopic();
    const event = generateEvent(topic);

    let value;
    if (topic === 'vehicle-completed') {
      // Avro-serialized with Apicurio wire format when registry is available;
      // fall back to Kafka Connect JSON schema envelope otherwise.
      value = avroGlobalId !== null
        ? serializeAvro(event, avroGlobalId)
        : Buffer.from(JSON.stringify(wrapWithSchema(event)));
    } else {
      // Plain JSON encoding for all other topics
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
    totalEventsSent++;
    if (eventCount % 10 === 0) {
      console.log(`[factory-producer] ${eventCount} events sent. Last: [${topic}] ${event.vin || event.partNumber || ''}`);
    }

    const profile = LOAD_PROFILES[currentLoadProfile];
    const delay = profile.minDelayMs + Math.floor(Math.random() * (profile.maxDelayMs - profile.minDelayMs + 1));
    await new Promise((r) => setTimeout(r, delay));
  }
}

run().catch((err) => {
  console.error('[factory-producer] Fatal error:', err);
  process.exit(1);
});
