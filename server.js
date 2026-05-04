import express from 'express';
import { Readable } from 'stream';

import streamChainPkg from 'stream-chain';
import streamJsonPkg from 'stream-json';
import pickPkg from 'stream-json/filters/Pick.js';
import streamArrayPkg from 'stream-json/streamers/StreamArray.js';

const { chain } = streamChainPkg;
const { parser } = streamJsonPkg;
const { pick } = pickPkg;
const { streamArray } = streamArrayPkg;

const app = express();
app.use(express.json({ limit: '1mb' }));

app.get('/', (req, res) => {
  res.send('OK');
});

app.post('/process', async (req, res) => {
  const { fileUrl, webhookUrl, batchSize = 100 } = req.body;

  if (!fileUrl || !webhookUrl) {
    return res.status(400).json({ error: 'Missing fileUrl or webhookUrl' });
  }

  res.json({ status: 'started' });

  try {
    const response = await fetch(fileUrl);

    if (!response.ok) {
      throw new Error(`File download failed: ${response.status}`);
    }

    const nodeStream = Readable.fromWeb(response.body);

    const pipeline = chain([
      nodeStream,
      parser(),
      pick({ filter: 'results' }),
      streamArray()
    ]);

    let batch = [];
    let batchIndex = 0;
    let total = 0;

    for await (const { value } of pipeline) {
      batch.push(value);
      total++;

      if (batch.length >= batchSize) {
        await sendBatch(webhookUrl, batch, batchIndex);
        batch = [];
        batchIndex++;
      }
    }

    if (batch.length > 0) {
      await sendBatch(webhookUrl, batch, batchIndex);
      batchIndex++;
    }

    await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'done', total, batches: batchIndex })
    });

  } catch (error) {
    console.error(error);

    await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'error', message: error.message })
    }).catch(() => {});
  }
});

async function sendBatch(url, orders, index) {
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      type: 'batch',
      batchIndex: index,
      count: orders.length,
      orders
    })
  });

  if (!response.ok) {
    throw new Error(`Webhook failed: ${response.status}`);
  }
}

const port = process.env.PORT || 3000;

app.listen(port, '0.0.0.0', () => {
  console.log(`Server running on ${port}`);
});
