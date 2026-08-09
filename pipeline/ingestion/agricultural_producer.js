import { Kafka } from "kafkajs";
import csv from "csv-parser";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";


const KAFKA_BROKER =
  process.env.KAFKA_SERVER ||
  "localhost:9092";

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

const TOPIC =
  process.env.KAFKA_TOPIC ||
  "raw-field-readings";

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

sendCSV();
