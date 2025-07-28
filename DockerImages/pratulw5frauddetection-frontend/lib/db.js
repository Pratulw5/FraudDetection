// lib/db.js
import { Pool } from 'pg';

export const pool = new Pool({
  connectionString: 'postgresql://flinkuser:flinkpass@postgres-service:5432/frauddb',
});
