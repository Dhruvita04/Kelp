const { Pool } = require('pg');
const config = require('./config');

const pool = new Pool({ 
  connectionString: config.databaseUrl,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

async function ensureTable() {
  const sql = `CREATE TABLE IF NOT EXISTS public.users (
    id serial4 PRIMARY KEY,
    "name" varchar NOT NULL,
    age int4 NOT NULL,
    address jsonb NULL,
    additional_info jsonb NULL
  );`;
  await pool.query(sql);
}

async function insertBatch(rows) {
  if (!rows || rows.length === 0) return;
  const values = [];
  const params = [];
  let idx = 1;
  for (const r of rows) {
    params.push(`($${idx++}, $${idx++}, $${idx++}, $${idx++})`);
    values.push(r.name, r.age, r.address ? JSON.stringify(r.address) : null, r.additional_info ? JSON.stringify(r.additional_info) : null);
  }
  const text = `INSERT INTO public.users ("name", age, address, additional_info) VALUES ${params.join(',')}`;
  await pool.query(text, values);
}

async function getAllAges() {
  const res = await pool.query('SELECT age FROM public.users');
  return res.rows.map(r => r.age);
}

async function close() {
  await pool.end();
}

module.exports = { pool, ensureTable, insertBatch, getAllAges, close };
