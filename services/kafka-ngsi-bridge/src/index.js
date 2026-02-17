import { Kafka } from "kafkajs";
import fetch from "node-fetch";

const brokers = (process.env.KAFKA_BROKERS ?? "kafka:9092").split(",");
const topic = process.env.KAFKA_TOPIC ?? "ngsi-ld";
const orionBaseUrl = process.env.ORION_LD_BASE_URL ?? "http://orion-ld:1026";
const orionTimeoutMs = Number(process.env.ORION_TIMEOUT_MS ?? "10000");

function toNgsiLdEntity(messageValue) {
  // Kafka message is already a full NGSI-LD entity JSON string
  return JSON.parse(messageValue);
}

function validateEntity(entity) {
  if (!entity || typeof entity !== "object") throw new Error("Invalid NGSI-LD payload: not an object");
  if (!entity.id || typeof entity.id !== "string") throw new Error("Invalid NGSI-LD payload: missing 'id'");
  if (!entity.type || typeof entity.type !== "string") throw new Error("Invalid NGSI-LD payload: missing 'type'");
}

async function upsertEntity(entity) {
  validateEntity(entity);

  const url = `${orionBaseUrl}/ngsi-ld/v1/entityOperations/upsert?options=update`;
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), orionTimeoutMs);

  // If the entity includes a JSON-LD @context, keep it.
  // Orion-LD accepts JSON-LD; use application/ld+json.
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/ld+json"
    },
    body: JSON.stringify([entity]),
    signal: controller.signal
  }).finally(() => clearTimeout(t));

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Orion-LD upsert failed: ${res.status} ${res.statusText} ${text}`);
  }
}

async function main() {
  const kafka = new Kafka({ clientId: "kafka-ngsi-bridge", brokers });
  const consumer = kafka.consumer({ groupId: "kafka-ngsi-bridge" });

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      const value = message.value.toString("utf8");
      const entity = toNgsiLdEntity(value);

      await upsertEntity(entity);
    }
  });
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});
