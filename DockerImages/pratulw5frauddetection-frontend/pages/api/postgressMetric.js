// pages/api/metrics.js
import { pool } from '../../lib/db';

export default async function handler(req, res) {
  if (req.method === 'GET') {
    try {
      const result = await pool.query(
        'SELECT * FROM metrics ORDER BY txn_id DESC LIMIT 1'
      );
      res.status(200).json(result.rows[0]);
    } catch (err) {
      res.status(500).json({ error: 'Failed to fetch metric' });
    }
  } else {
    res.status(405).json({ message: 'Method not allowed' });
  }
}
