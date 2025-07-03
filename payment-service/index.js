import express from "express";
import cors from "cors";
import { Kafka } from "kafkajs";

const app = express();
const PORT = process.env.PORT || 8000;

app.use(
  cors({
    origin: "*",
    methods: ["GET", "POST", "PUT", "DELETE"],
  })
);

app.use(express.json());

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9094", "localhost:9095", "localhost:9096"],
});

const producer = kafka.producer();

const connectToKafka = async () => {
  try {
    await producer.connect();
    console.log("Connected to Kafka Producer");
  } catch (error) {
    console.error("Error connecting to Kafka Producer:", error);
  }
};

app.post("/payment-service", async (req, res) => {
  const { cart } = req.body;

  const userId = "123";

  //TODO:PAYMENT
  console.log("API endpoint hit for payment service");

  //KAFKA
  await producer.send({
    topic: "payment-successful",
    messages: [
      {
        value: JSON.stringify({
          userId,
          cart,
        }),
      },
    ],
  });

  return res.status(200).send("Payment successful");
});

app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).send("Something broke!");
});

app.listen(PORT, () => {
  connectToKafka();
  console.log(`Payment service is running on port ${PORT}`);
});
