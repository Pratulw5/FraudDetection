const fs = require('fs');
const csv = require('csv-parser');
const { Kafka, CompressionTypes } = require('kafkajs');

console.log("🚀 Kafka producer starting...");

const kafka = new Kafka({
  clientId: 'transaction-producer',
  brokers: ['kafka:9092'],
});
const producer = kafka.producer();

function parseTimestamp(ts) {
  const [datePart, timePart] = ts.split(' ');
  const [dd, mm, yyyy] = datePart.split('/');
  return new Date(`${yyyy}-${mm}-${dd} ${timePart}`).getTime(); // milliseconds
}

let firstTxnTime = null;
let realStartTime = null;

async function sendRow(row) {
  const txnTime = parseTimestamp(row.timestamp);

  if (!firstTxnTime) {
    firstTxnTime = txnTime;
    realStartTime = Date.now();
  }

  const intendedDelay = txnTime - firstTxnTime;
  const actualTime = Date.now();
  const targetTime = realStartTime + intendedDelay;
  const delay = targetTime - actualTime;

  // Wait for delay if needed
  if (delay > 0) {
    await new Promise(res => setTimeout(res, delay));
  }

  try {
    await producer.send({
      topic: 'raw-transactions',
      compression: CompressionTypes.GZIP,
      messages: [{ value: JSON.stringify(row) }],
    });
    console.log(`✅ Sent txn at ${row.timestamp}`);
  } catch (err) {
    console.error(`❌ Error sending txn at ${row.timestamp}:`, err.message);
  }
}

async function run() {
  await producer.connect();
  console.log("📡 Kafka producer connected. Reading CSV...");

  const rows = [];

  // Step 1: Read and store all rows
  await new Promise((resolve, reject) => {
    fs.createReadStream('data.csv')
      .pipe(csv())
      .on('data', (row) => rows.push(row))
      .on('end', resolve)
      .on('error', reject);
  });

  // Step 2: Sort rows by timestamp (just in case)
  rows.sort((a, b) => parseTimestamp(a.timestamp) - parseTimestamp(b.timestamp));

  // Step 3: Send rows in sequence with real-time delay
  firstTxnTime = parseTimestamp(rows[0].timestamp);
  realStartTime = Date.now();

  for (const row of rows) {
    await sendRow(row);  // Await ensures proper delay per row
  }

  console.log("✅ Finished sending all transactions.");
  await producer.disconnect();
  console.log("🔌 Kafka producer disconnected.");
}


run().catch(console.error);
