// QA FIXTURE for VulnGraph cross-repo taint validation. Synthetic service, never deployed.
const express = require("express");
const jwt = require("jsonwebtoken");
const { Kafka } = require("kafkajs");

const app = express();
app.use(express.json());

const kafka = new Kafka({ clientId: "order-service", brokers: ["kafka:9092"] });
const producer = kafka.producer();

function requireAuth(req, res, next) {
  try {
    jwt.verify(req.headers.authorization?.replace("Bearer ", ""), process.env.JWT_SECRET);
    next();
  } catch {
    res.status(401).json({ error: "unauthorized" });
  }
}

// Taint source: authenticated REST endpoint accepting a request body + path param.
app.post("/orders/:userId", requireAuth, async (req, res) => {
  const order = { userId: req.params.userId, card: req.body.card, sku: req.body.sku };
  // Fan-out to downstream consumers over the shared "order-events" topic.
  await producer.send({
    topic: "order-events",
    messages: [{ key: order.userId, value: JSON.stringify(order) }],
  });
  res.status(202).json({ queued: true });
});

app.listen(3000, () => console.log("order-service on :3000"));
