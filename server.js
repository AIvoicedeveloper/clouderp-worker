import express from 'express';
import { chain } from 'stream-chain';
import { parser } from 'stream-json';
import { pick } from 'stream-json/filters/Pick.js';
import { streamArray } from 'stream-json/streamers/StreamArray.js';

const app = express();
app.use(express.json());

// teszt endpoint
app.get('/', (req, res) => {
  res.send('OK');
});

app.post('/process', async (req, res) => {
  const { fileUrl, webhookUrl, batchSize = 100 } = req.body;

  if (!fileUrl || !webhookUrl) {
    return res.status(400).json({ error: 'Missing params' });
  }

  res.json({ status: 'started' });

  const response = await fetch(fileUrl);

  const pipeline = chain([
    response.body,
    parser(),
    pick({ filter: 'results' }),
    streamArray()
  ]);

  let batch = [];
  let batchIndex = 0;

  for await (const { value } of pipeline) {
    batch.push(value);

    if (batch.length >= batchSize) {
      await sendBatch(webhookUrl, batch, batchIndex);
      batch = [];
      batchIndex++;
    }
  }

  if (batch.length > 0) {
    await sendBatch(webhookUrl, batch, batchIndex);
  }
});

async function sendBatch(url, orders, index) {
  await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      type: 'batch',
      batchIndex: index,
      orders
    })
  });
}

const port = process.env.PORT || 3000;

app.listen(port, () => {
  console.log(`Server running on ${port}`);
});
