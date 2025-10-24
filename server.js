
require('dotenv').config();
const fs = require('fs');
const readline = require('readline');
const express = require('express');
const db = require('./db');
const { parseCSVLine, buildObjectFromRow } = require('./csvParser');

const CSV_PATH = process.env.CSV_FILE_PATH || './data/users.csv';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '500', 10);

const app = express();
app.use(express.json());

async function batchInsert(rows) {
  if (rows.length === 0) return;
  const client = await db.getClient();
  try {
    await client.query('BEGIN');
    const values = [];
    const placeholders = [];
    let idx = 1;

    rows.forEach(r => {
      placeholders.push(`($${idx++}, $${idx++}, $${idx++}, $${idx++})`);
      values.push(r.name);
      values.push(r.age);
      values.push(r.address ? JSON.stringify(r.address) : null);
      values.push(r.additional_info ? JSON.stringify(r.additional_info) : null);
    });

    const sql = `INSERT INTO public.users("name", age, address, additional_info)
                 VALUES ${placeholders.join(',')}`;
    await client.query(sql, values);
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

function extractFields(obj) {
  const firstName = obj.name?.firstName || '';
  const lastName = obj.name?.lastName || '';
  const name = `${firstName} ${lastName}`.trim() || null;
  const age = obj.age ? parseInt(obj.age) : null;
  const address = obj.address && Object.keys(obj.address).length ? obj.address : null;

  const additional_info = {};
  for (const key in obj) {
    if (key !== 'name' && key !== 'age' && key !== 'address') {
      additional_info[key] = obj[key];
    }
  }
  return { name, age, address, additional_info: Object.keys(additional_info).length ? additional_info : null };
}

async function processCSVFile(path) {
  if (!fs.existsSync(path)) throw new Error('CSV file not found at ' + path);

  const rl = readline.createInterface({
    input: fs.createReadStream(path),
    crlfDelay: Infinity
  });

  let headers = null;
  const batch = [];
  let total = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;
    if (!headers) {
      headers = parseCSVLine(line);
      continue;
    }
    const values = parseCSVLine(line);
    while (values.length < headers.length) values.push('');
    const obj = buildObjectFromRow(headers, values);
    const extracted = extractFields(obj);
    if (!extracted.name || !extracted.age) continue;

    batch.push(extracted);
    total++;

    if (batch.length >= BATCH_SIZE) {
      await batchInsert(batch.splice(0, batch.length));
    }
  }

  if (batch.length) await batchInsert(batch);
  return total;
}

async function printAgeDistribution() {
  const res = await db.query('SELECT age, COUNT(*) as cnt FROM public.users GROUP BY age');
  const rows = res.rows;
  let total = 0;
  rows.forEach(r => total += parseInt(r.cnt));

  const groups = { '<20': 0, '20-40': 0, '40-60': 0, '>60': 0 };
  rows.forEach(r => {
    const age = parseInt(r.age);
    const cnt = parseInt(r.cnt);
    if (age < 20) groups['<20'] += cnt;
    else if (age <= 40) groups['20-40'] += cnt;
    else if (age <= 60) groups['40-60'] += cnt;
    else groups['>60'] += cnt;
  });

  console.log('Age-Group % Distribution');
  for (const [range, count] of Object.entries(groups)) {
    const pct = total ? Math.round((count / total) * 100) : 0;
    switch (range) {
      case '<20': console.log(`< 20 ${pct}`); break;
      case '20-40': console.log(`20 to 40 ${pct}`); break;
      case '40-60': console.log(`40 to 60 ${pct}`); break;
      case '>60': console.log(`> 60 ${pct}`); break;
    }
  }
}

app.post('/process', async (req, res) => {
  try {
    const count = await processCSVFile(CSV_PATH);
    await printAgeDistribution();
    res.json({ status: 'ok', processed: count });
  } catch (err) {
    console.error(err);
    res.status(500).json({ status: 'error', message: err.message });
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Server running on port ${port}`));
