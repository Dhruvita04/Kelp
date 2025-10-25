const dotenv = require('dotenv');
const path = require('path');

dotenv.config();

const config = {
  csvFilePath: process.env.CSV_FILE_PATH || path.join(__dirname, '..', 'sample', 'data.csv'),
  databaseUrl: process.env.DATABASE_URL,
  insertBatchSize: parseInt(process.env.INSERT_BATCH_SIZE || '1000', 10),
  port: parseInt(process.env.PORT || '3000', 10),
  dryRun: (process.env.DRY_RUN === 'true') || false,
};

module.exports = config;
