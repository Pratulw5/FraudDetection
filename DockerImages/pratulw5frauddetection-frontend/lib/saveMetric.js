// lib/saveMetric.js
import { pool } from './db.js';

export async function saveMetricToDB(metric) {
  const {
    txn_id,timestamp, tp, fp, tn, fn,
    precision, recall, accuracy, f1_score
  } = metric;

  await pool.query(`
    INSERT INTO metrics (txn_id, timestamp, tp, fp, tn, fn, precision, recall, accuracy, f1_score)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
  `, [txn_id, timestamp, tp, fp, tn, fn, precision, recall, accuracy, f1_score]);
}
