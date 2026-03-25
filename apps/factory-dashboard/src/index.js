'use strict';

const fs = require('fs');
const https = require('https');
const express = require('express');
const path    = require('path');
const { startConsumer } = require('./consumer');

const HTTP_PORT = parseInt(process.env.HTTP_PORT || '3000', 10);
const WS_PORT   = parseInt(process.env.WS_PORT   || '3001', 10);
const PRODUCER_CONTROL_URL = process.env.PRODUCER_CONTROL_URL || 'http://factory-producer-control.kafka-car-factory.svc.cluster.local:3002';
const DASHBOARD_NAMESPACE = process.env.DASHBOARD_NAMESPACE || 'kafka-car-factory';
const BROKER_NODEPOOL_NAME = process.env.BROKER_NODEPOOL_NAME || 'broker';
const BROKER_SCALE_UP_REPLICAS = parseInt(process.env.BROKER_SCALE_UP_REPLICAS || '4', 10);
const BROKER_SCALE_BASE_REPLICAS = parseInt(process.env.BROKER_SCALE_BASE_REPLICAS || '3', 10);
const AUTO_SCALE_HIGH_WATER = parseInt(process.env.AUTO_SCALE_HIGH_WATER || '5', 10);   // events/sec → scale up
const AUTO_SCALE_LOW_WATER  = parseInt(process.env.AUTO_SCALE_LOW_WATER  || '2', 10);   // events/sec → scale down
const AUTO_SCALE_COOLDOWN_MS = parseInt(process.env.AUTO_SCALE_COOLDOWN_SECONDS || '60', 10) * 1000;
const AUTO_SCALE_INTERVAL_MS = parseInt(process.env.AUTO_SCALE_INTERVAL_SECONDS || '10', 10) * 1000;
const SERVICE_ACCOUNT_DIR = '/var/run/secrets/kubernetes.io/serviceaccount';

// ─── Auto-scale state ────────────────────────────────────────────────────────
let autoScaleLastEventsTotal = null;
let autoScaleLastCheckAt = null;
let autoScaleLastEventsPerSec = null;
let autoScaleCurrentTarget = BROKER_SCALE_BASE_REPLICAS;
let autoScaleCooldownUntil = 0;

function getServiceAccountToken() {
  try {
    return fs.readFileSync(path.join(SERVICE_ACCOUNT_DIR, 'token'), 'utf8').trim();
  } catch (_error) {
    return null;
  }
}

function getServiceAccountCa() {
  try {
    return fs.readFileSync(path.join(SERVICE_ACCOUNT_DIR, 'ca.crt'));
  } catch (_error) {
    return null;
  }
}

function kubernetesRequest(method, apiPath, payload) {
  const token = getServiceAccountToken();
  const ca = getServiceAccountCa();
  const host = process.env.KUBERNETES_SERVICE_HOST;
  const port = parseInt(process.env.KUBERNETES_SERVICE_PORT || '443', 10);

  if (!token || !ca || !host) {
    return Promise.reject(new Error('Kubernetes API access is not configured inside this pod'));
  }

  const body = payload ? JSON.stringify(payload) : null;
  const headers = {
    Authorization: `Bearer ${token}`,
    Accept: 'application/json',
  };

  if (body) {
    headers['Content-Type'] = 'application/merge-patch+json';
    headers['Content-Length'] = Buffer.byteLength(body);
  }

  return new Promise((resolve, reject) => {
    const req = https.request(
      {
        hostname: host,
        port,
        path: apiPath,
        method,
        ca,
        headers,
      },
      (response) => {
        let responseBody = '';
        response.on('data', (chunk) => {
          responseBody += chunk;
        });
        response.on('end', () => {
          const parsed = responseBody ? JSON.parse(responseBody) : {};
          if (response.statusCode >= 200 && response.statusCode < 300) {
            resolve(parsed);
            return;
          }

          reject(new Error(parsed.message || `Kubernetes API request failed with status ${response.statusCode}`));
        });
      }
    );

    req.on('error', reject);

    if (body) {
      req.write(body);
    }

    req.end();
  });
}

function getBrokerNodePoolPath() {
  return `/apis/kafka.strimzi.io/v1/namespaces/${DASHBOARD_NAMESPACE}/kafkanodepools/${BROKER_NODEPOOL_NAME}`;
}

async function fetchProducerLoadState() {
  const response = await fetch(`${PRODUCER_CONTROL_URL}/control/state`);
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || 'Unable to read producer load state');
  }
  return payload;
}

async function fetchBrokerScaleState() {
  const nodePool = await kubernetesRequest('GET', getBrokerNodePoolPath());
  return {
    replicas: nodePool.spec?.replicas ?? null,
    name: nodePool.metadata?.name || BROKER_NODEPOOL_NAME,
  };
}

async function patchBrokerReplicas(replicas) {
  const nodePool = await kubernetesRequest('PATCH', getBrokerNodePoolPath(), { spec: { replicas } });
  return {
    replicas: nodePool.spec?.replicas ?? replicas,
    name: nodePool.metadata?.name || BROKER_NODEPOOL_NAME,
  };
}

async function autoScaleLoop() {
  try {
    const producer = await fetchProducerLoadState();
    const now = Date.now();
    const eventsTotal = producer.eventsTotal ?? null;

    if (eventsTotal !== null && autoScaleLastEventsTotal !== null && autoScaleLastCheckAt !== null) {
      const elapsed = (now - autoScaleLastCheckAt) / 1000;
      autoScaleLastEventsPerSec = elapsed > 0 ? (eventsTotal - autoScaleLastEventsTotal) / elapsed : null;
    }

    autoScaleLastEventsTotal = eventsTotal;
    autoScaleLastCheckAt = now;

    if (autoScaleLastEventsPerSec === null) return; // need two samples before acting

    const inCooldown = now < autoScaleCooldownUntil;
    if (inCooldown) return;

    let targetReplicas = autoScaleCurrentTarget;
    if (autoScaleLastEventsPerSec > AUTO_SCALE_HIGH_WATER && autoScaleCurrentTarget < BROKER_SCALE_UP_REPLICAS) {
      targetReplicas = BROKER_SCALE_UP_REPLICAS;
    } else if (autoScaleLastEventsPerSec < AUTO_SCALE_LOW_WATER && autoScaleCurrentTarget > BROKER_SCALE_BASE_REPLICAS) {
      targetReplicas = BROKER_SCALE_BASE_REPLICAS;
    }

    if (targetReplicas !== autoScaleCurrentTarget) {
      console.log(
        `[auto-scale] events/sec=${autoScaleLastEventsPerSec.toFixed(2)}, ` +
        `scaling brokers ${autoScaleCurrentTarget} → ${targetReplicas}`
      );
      await patchBrokerReplicas(targetReplicas);
      autoScaleCurrentTarget = targetReplicas;
      autoScaleCooldownUntil = now + AUTO_SCALE_COOLDOWN_MS;
    }
  } catch (error) {
    console.error('[auto-scale] Loop error:', error.message);
  }
}

function buildDemoState(producer, broker, brokerError) {
  const eventsPerSec = autoScaleLastEventsPerSec !== null ? parseFloat(autoScaleLastEventsPerSec.toFixed(1)) : null;
  const inCooldown = Date.now() < autoScaleCooldownUntil;
  const brokerReplicas = broker?.replicas ?? autoScaleCurrentTarget;

  let bannerTone;
  let bannerText;
  if (brokerError) {
    bannerTone = 'warning';
    bannerText = `${producer.label} — broker scaling unavailable: ${brokerError}`;
  } else if (producer.profile === 'heavy') {
    bannerTone = 'heavy';
    bannerText = `Heavy load active — ${brokerReplicas} broker(s)` +
      (eventsPerSec !== null ? ` — ${eventsPerSec} events/sec` : '') +
      (inCooldown ? ' — cooldown active' : '');
  } else {
    bannerTone = 'normal';
    bannerText = `Normal load active — ${brokerReplicas} broker(s)` +
      (eventsPerSec !== null ? ` — ${eventsPerSec} events/sec` : '') +
      (inCooldown ? ' — cooldown active' : '');
  }

  return {
    producer,
    broker: broker || null,
    brokerError: brokerError || null,
    bannerTone,
    bannerText,
    autoScale: {
      eventsPerSec,
      currentTarget: autoScaleCurrentTarget,
      highWater: AUTO_SCALE_HIGH_WATER,
      lowWater: AUTO_SCALE_LOW_WATER,
      inCooldown,
    },
  };
}

const app = express();

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Inject the WebSocket port into the HTML at runtime
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/health', (_req, res) => res.json({ status: 'ok' }));

app.get('/config', (_req, res) => res.json({ wsPort: WS_PORT }));

app.get('/api/load-state', async (_req, res) => {
  try {
    const producer = await fetchProducerLoadState();
    try {
      const broker = await fetchBrokerScaleState();
      res.json(buildDemoState(producer, broker, null));
    } catch (brokerError) {
      res.json(buildDemoState(producer, null, brokerError.message));
    }
  } catch (error) {
    res.status(502).json({ error: 'Unable to reach producer control API' });
  }
});

app.post('/api/load-profile', async (req, res) => {
  try {
    const response = await fetch(`${PRODUCER_CONTROL_URL}/control/load-profile`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ profile: req.body.profile }),
    });
    const producer = await response.json();
    if (!response.ok) {
      res.status(response.status).json(producer);
      return;
    }

    // Reset auto-scale sampling so the loop gets a fresh rate measurement
    // after the producer throughput changes.  Broker scaling is now handled
    // exclusively by autoScaleLoop().
    autoScaleLastEventsTotal = null;
    autoScaleLastCheckAt = null;
    autoScaleLastEventsPerSec = null;
    autoScaleCooldownUntil = 0;

    try {
      const broker = await fetchBrokerScaleState();
      res.json(buildDemoState(producer, broker, null));
    } catch (brokerError) {
      res.json(buildDemoState(producer, null, brokerError.message));
    }
  } catch (error) {
    res.status(502).json({ error: 'Unable to update producer load profile' });
  }
});

app.listen(HTTP_PORT, () => {
  console.log(`[dashboard] HTTP server listening on port ${HTTP_PORT}`);
});

// Kick off the auto-scale polling loop
setInterval(autoScaleLoop, AUTO_SCALE_INTERVAL_MS);
console.log(
  `[auto-scale] Polling every ${AUTO_SCALE_INTERVAL_MS / 1000}s — ` +
  `scale up >${AUTO_SCALE_HIGH_WATER} evt/s, scale down <${AUTO_SCALE_LOW_WATER} evt/s, ` +
  `cooldown ${AUTO_SCALE_COOLDOWN_MS / 1000}s`
);

startConsumer().catch((err) => {
  console.error('[dashboard] Consumer error:', err);
  process.exit(1);
});
