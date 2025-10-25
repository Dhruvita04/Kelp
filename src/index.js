const express = require('express');
const config = require('./config');
const { processFile } = require('./process');
const db = require('./db');

const app = express();

app.get('/', (req, res) => res.send('CSV to JSON uploader running'));

app.post('/process', async (req, res) => {
  try {
    await processFile(config.csvFilePath);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

async function start() {
  const server = app.listen(config.port, () => {
    console.log(`Server listening on port ${config.port}`);
  });

  if (process.argv[2] === 'process') {
    try {
      console.log('Processing CSV at', config.csvFilePath);
      await processFile(config.csvFilePath);
    } catch (err) {
      console.error('Error processing file:', err);
    } finally {
      await db.close();
      server.close(() => process.exit(0));
    }
  }
}

start().catch(err => {
  console.error('Startup error', err);
  process.exit(1);
});
