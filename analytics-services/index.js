import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "analytics-service",
  brokers: ["localhost:9094", "localhost:9095", "localhost:9096"],
});

const consumer = kafka.consumer({ groupId: "analytics-service" });

const run = async () => {
  try {
    await consumer.connect();

    await consumer.subscribe({
      topics: ["payment-successful", "order-successful", "email-successful"],
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        switch (topic) {
          case "payment-successful":
            {
              const value = message.value.toString();
              console.log(`Received message: ${value} from topic: ${topic}`);

              const { userId, cart } = JSON.parse(value);

              const total = cart
                .reduce((acc, item) => acc + item.price, 0)
                .toFixed(2);

              console.log(
                `Analytics for user ${userId}: Total payment amount is ${total}`
              );
            }
            break;

          case "order-successful":
            {
              const value = message.value.toString();
              console.log(`Received message: ${value} from topic: ${topic}`);

              const { userId, oderId } = JSON.parse(value);

              console.log(
                `Analytics for user ${userId}: Order ID ${oderId} has been successfully created`
              );
            }
            break;

          case "email-successful":
            {
              const value = message.value.toString();
              const { userId, emailId } = JSON.parse(value);

              console.log(`Analytic consumer: Email id ${emailId} sent to user id ${userId}`);
            }
            break;

          default:
            console.warn(`Unknown topic: ${topic}`);
        }

        // Here you can process the message and perform analytics operations
        // For example, you could save the data to a database or perform calculations
      },
    });
  } catch (error) {
    console.error("Error connecting to Kafka Producer:", error);
  }
};

run();
