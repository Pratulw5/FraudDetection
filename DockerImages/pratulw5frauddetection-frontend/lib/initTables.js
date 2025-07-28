import { pool } from './db.js';
export async function createTables() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS metrics (
        id SERIAL PRIMARY KEY,
        txn_id VARCHAR NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        tp INTEGER,
        fp INTEGER,
        tn INTEGER,
        fn INTEGER,
        precision FLOAT,
        recall FLOAT,
        accuracy FLOAT,
        f1_score FLOAT
      );
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS prediction (
        id SERIAL PRIMARY KEY,
        txn_id VARCHAR NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        score_rule1 FLOAT,
        score_rule2 FLOAT,
        score_rule3 FLOAT,
        score_rule4 FLOAT,
        score_rule5 FLOAT,
        score_rule6 FLOAT,
        score_rule7 FLOAT,
        score_rule8 FLOAT,
        Rules_Check TEXT,
        isFraud BOOLEAN,
        fraud_prediction BOOLEAN,
        fraud_score FLOAT
      );
    `);

    console.log("✅ Tables created successfully");
  } catch (err) {
    console.error("❌ Failed to create tables:", err);
  }
}