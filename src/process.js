const { streamCsv } = require('./parser');
const db = require('./db');
const config = require('./config');
const fs = require('fs');
const path = require('path');
const { pipeline } = require('stream');
const { promisify } = require('util');

const pipelineAsync = promisify(pipeline);

function setNested(obj, parts, value) {
  let cur = obj;
  for (let i = 0; i < parts.length; i++) {
    const p = parts[i];
    if (i === parts.length - 1) {
      cur[p] = value;
    } else {
      if (!cur[p] || typeof cur[p] !== 'object') cur[p] = {};
      cur = cur[p];
    }
  }
}

function buildObjectFromRow(header, values) {
  const obj = {};
  for (let i = 0; i < header.length; i++) {
    const key = header[i];
    const val = values[i] === undefined ? null : values[i];
    if (val === null) continue;
    const parts = key.split('.');
    setNested(obj, parts, val);
  }
  return obj;
}

function mapToDbRecord(obj) {
  const firstName = (((obj.name || {}).firstName) || '').toString();
  const lastName = (((obj.name || {}).lastName) || '').toString();
  const ageRaw = obj.age || obj.age === 0 ? obj.age : null;
  const age = ageRaw !== null ? parseInt(ageRaw, 10) : null;

  if (!firstName || !lastName || Number.isNaN(age) || age === null) {
    throw new Error('Missing mandatory fields (name.firstName, name.lastName, age)');
  }

  const name = (firstName + ' ' + lastName).trim();
  const address = obj.address || null;
  const additional_info = JSON.parse(JSON.stringify(obj));
  delete additional_info.name;
  delete additional_info.age;
  delete additional_info.address;

  const aiKeys = Object.keys(additional_info);
  const addInfoFinal = aiKeys.length === 0 ? null : additional_info;

  return { name, age, address, additional_info: addInfoFinal };
}

async function processFile(filePath) {
  await db.ensureTable();
  const batch = [];
  const outputPath = path.join(path.dirname(filePath), 'output.json');
  const additionalInfoPath = path.join(path.dirname(filePath), 'additional-info.json');

  const writeStream = fs.createWriteStream(outputPath, { encoding: 'utf8' });
  writeStream.write('[\n'); 
  
  const additionalInfoStream = fs.createWriteStream(additionalInfoPath, { encoding: 'utf8' });
  additionalInfoStream.write('[\n');
  
  let total = 0;
  let success = 0;
  let failed = 0;
  let isFirstRecord = true;
  let isFirstAdditional = true;
  const ages = [];

  await streamCsv(filePath, null, (header, values) => {
    total++;
    try {
      const obj = buildObjectFromRow(header, values);
      
      if (!isFirstRecord) {
        writeStream.write(',\n');
      }
      writeStream.write('  ' + JSON.stringify(obj));
      isFirstRecord = false;
      
      const rec = mapToDbRecord(obj);
      ages.push(rec.age);
      batch.push(rec);
      
      if (rec.additional_info) {
        if (!isFirstAdditional) {
          additionalInfoStream.write(',\n');
        }
        const additionalRecord = {
          recordIndex: total - 1,
          name: rec.name,
          additional_info: rec.additional_info
        };
        additionalInfoStream.write('  ' + JSON.stringify(additionalRecord));
        isFirstAdditional = false;
      }
      
    } catch (err) {
      failed++;
      console.error(`Skipping row ${total} due to error: ${err.message}`);
    }
  });

  const totalBatches = Math.ceil(batch.length / config.insertBatchSize);
  for (let i = 0; i < totalBatches; i++) {
    const start = i * config.insertBatchSize;
    const end = Math.min(start + config.insertBatchSize, batch.length);
    const currentBatch = batch.slice(start, end);
    await db.insertBatch(currentBatch);
    success += currentBatch.length;
  }

  writeStream.write('\n]');
  writeStream.end();
  
  additionalInfoStream.write('\n]');
  additionalInfoStream.end();
  
  await Promise.all([
    new Promise((resolve, reject) => {
      writeStream.on('finish', resolve);
      writeStream.on('error', reject);
    }),
    new Promise((resolve, reject) => {
      additionalInfoStream.on('finish', resolve);
      additionalInfoStream.on('error', reject);
    })
  ]);
  
  console.log(`JSON output saved to: ${outputPath}`);
  console.log(`Additional info saved to: ${additionalInfoPath}`);

  printAgeDistribution(ages);
}

function printAgeDistribution(ages) {
  const total = ages.length;
  const groups = { lt20: 0, g20_40: 0, g40_60: 0, gt60: 0 };
  for (const a of ages) {
    if (a < 20) groups.lt20++;
    else if (a >= 20 && a <= 40) groups.g20_40++;
    else if (a > 40 && a <= 60) groups.g40_60++;
    else groups.gt60++;
  }

  function pct(n) {
    return total === 0 ? 0 : Math.round((n * 100) / total);
  }

  console.log('Age-Group % Distribution');
  console.log(`< 20: ${pct(groups.lt20)}`);
  console.log(`20 to 40: ${pct(groups.g20_40)}`);
  console.log(`40 to 60: ${pct(groups.g40_60)}`);
  console.log(`> 60: ${pct(groups.gt60)}`);
}

module.exports = { processFile };
