// lib/saveMetric.js
import { pool } from './db.js';

export async function savePredictionToDB(prediction) {
  const { txn_id, timestamp, score_rule1, score_rule2, score_rule3, score_rule4, score_rule5, score_rule6, score_rule7, score_rule8, Rules_Check, isFraud, fraud_prediction, fraud_score } = prediction;

  await pool.query(`
    INSERT INTO prediction (txn_id, timestamp, score_rule1, score_rule2, score_rule3, score_rule4, score_rule5, score_rule6, score_rule7, score_rule8, Rules_Check, isFraud, fraud_prediction, fraud_score)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
  `, [txn_id, timestamp, score_rule1, score_rule2, score_rule3, score_rule4, score_rule5, score_rule6, score_rule7, score_rule8, Rules_Check, isFraud, fraud_prediction, fraud_score]);
}
