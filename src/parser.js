const fs = require('fs');
const readline = require('readline');

function parseCsvLine(line) {
  const res = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += ch;
      }
    } else {
      if (ch === ',') {
        res.push(cur);
        cur = '';
      } else if (ch === '"') {
        inQuotes = true;
      } else {
        cur += ch;
      }
    }
  }
  res.push(cur);
  return res;
}

async function streamCsv(filePath, onHeader, onRow) {
  return new Promise((resolve, reject) => {
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath),
      crlfDelay: Infinity,
    });

    let isFirst = true;
    let header = null;
    rl.on('line', (line) => {
      if (line.trim() === '') return;
      if (isFirst) {
        header = parseCsvLine(line).map(h => h.trim());
        isFirst = false;
        if (onHeader) onHeader(header);
      } else {
        const values = parseCsvLine(line).map(v => v.trim());
        try {
          onRow(header, values);
        } catch (err) {
          rl.close();
          reject(err);
        }
      }
    });
    rl.on('close', () => resolve());
    rl.on('error', reject);
  });
}

module.exports = { parseCsvLine, streamCsv };
