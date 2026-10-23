// Preserves normal Field ingestion while supporting evaluation settings



import { Kafka } from "kafkajs";
import csv from "csv-parser";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadEvaluationEnv } from "../common/load-evaluation-env.js";




loadEvaluationEnv();
const KAFKA_BROKER = process.env.KAFKA_SERVER || "localhost:9092";

const kafka = new Kafka({
  clientId: "agri-producer",
  brokers: [KAFKA_BROKER],
});

const producer = kafka.producer();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const csvPath = path.resolve(
  __dirname,
  "../../data/source/Smart_Farming_Crop_Yield_2024.csv",
);

const TOPIC = process.env.KAFKA_TOPIC || "raw-field-readings";

console.log("Reading CSV from:", csvPath);
console.log(`Kafka broker: ${KAFKA_BROKER}`);
console.log(`Kafka topic: ${TOPIC}`);

async function sendCSV() {
  const rows = [];

  fs.createReadStream(csvPath)
    .pipe(csv())
    .on("data", (row) => {
      rows.push(row);
    })
    .on("end", async () => {
      console.log(`Loaded ${rows.length} records`);

      await producer.connect();

      const messages = rows.map((row, index) => ({
        key: String(index + 1),
        value: JSON.stringify(row),
      }));

      await producer.send({
        topic: TOPIC,
        messages,
      });

      console.log(`Sent ${messages.length} agricultural records`);

      await producer.disconnect();
    });
}

async function sendStdin() {
  const readline = await import("node:readline");
  const lines = readline.createInterface({
    input: process.stdin,
    crlfDelay: Infinity,
  });
  const batchSize = Math.max(1, Number(process.env.PRODUCER_BATCH_SIZE || 500));
  let batch = [],
    total = 0;
  await producer.connect();
  try {
    for await (const line of lines) {
      if (!line.trim()) continue;
      const record = JSON.parse(line);
      batch.push({
        key: String(record.farm_id),
        value: JSON.stringify(record),
      });
      if (batch.length >= batchSize) {
        await producer.send({ topic: TOPIC, messages: batch });
        total += batch.length;
        batch = [];
      }
    }
    if (batch.length) {
      await producer.send({ topic: TOPIC, messages: batch });
      total += batch.length;
    }
    console.log(`Sent ${total} field records in batches`);
  } finally {
    await producer.disconnect();
  }
}

if (process.argv.includes("--stdin")) {
  sendStdin().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
} else {
  sendCSV();
}
