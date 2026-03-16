'use strict';

const express = require('express');
const path    = require('path');
const { startConsumer } = require('./consumer');

const HTTP_PORT = parseInt(process.env.HTTP_PORT || '3000', 10);
const WS_PORT   = parseInt(process.env.WS_PORT   || '3001', 10);

const app = express();

app.use(express.static(path.join(__dirname, 'public')));

// Inject the WebSocket port into the HTML at runtime
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/health', (_req, res) => res.json({ status: 'ok' }));

app.get('/config', (_req, res) => res.json({ wsPort: WS_PORT }));

app.listen(HTTP_PORT, () => {
  console.log(`[dashboard] HTTP server listening on port ${HTTP_PORT}`);
});

startConsumer().catch((err) => {
  console.error('[dashboard] Consumer error:', err);
  process.exit(1);
});
