const fs = require('fs');
const csv = require('csv-parser');
const { Kafka, CompressionTypes } = require('kafkajs');
console.log("🚀 Kafka producer starting...");
// Kafka setup
const kafka = new Kafka({
  clientId: 'transaction-producer',
  brokers: ['kafka:9092'], // Make sure this matches your Kafka host
});
const producer = kafka.producer();

// Stream rows to Kafka
function streamRows(rows) {

  let index = 0;

  async function sendNextBatch() {
    if (index >= rows.length) {
      console.log("✅ All transactions sent.");
      await producer.disconnect();
      return;
    }

    const currentRow = rows[index];
    const currentTimestamp = new Date(currentRow.timestamp.split("/").reverse().join("-")).getTime() / 1000; // Convert to seconds
    const batch = [currentRow];
    index++;

    // Add all transactions with the same timestamp
    while (
      index < rows.length &&
      new Date(rows[index].timestamp.split("/").reverse().join("-")).getTime() / 1000 === currentTimestamp // Compare timestamps in seconds
    ) {
      batch.push(rows[index]);
      index++;
    }

    try {
      await producer.send({
        topic: 'raw-transactions',
        compression: CompressionTypes.GZIP,
        messages: batch.map(row => ({ value: JSON.stringify(row) })),
      });
      console.log(`✅ Sent ${batch.length} transaction(s) for timestamp: ${currentRow.timestamp}`);
    } catch (err) {
      console.error(`❌ Error sending batch for timestamp ${currentRow.timestamp}:`, err.message);
    }

    // If there are more rows to send, calculate delay based on the next timestamp difference in seconds
    if (index < rows.length) {
      const nextTimestamp = new Date(rows[index].timestamp.split("/").reverse().join("-")).getTime() / 1000; // Convert to seconds
      const delay = Math.max(0, (nextTimestamp - currentTimestamp) * 1000); // Delay in milliseconds
      setTimeout(sendNextBatch, delay);
    } else {
      console.log("✅ Finished all batches.");
      await producer.disconnect();
    }
  }

  sendNextBatch();
}

// Read CSV and start
async function run() {
  const rows = [];

  // Read the CSV file
  fs.createReadStream('data.csv')
    .pipe(csv())
    .on('data', (row) => {
      rows.push(row);
    })
    .on('end', async () => {
      console.log(`📄 Read ${rows.length} rows from data.csv`);
      await producer.connect();
      streamRows(rows);
    });
}

run().catch(console.error);
