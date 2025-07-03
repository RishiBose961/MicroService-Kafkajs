import {Kafka} from 'kafkajs';

const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:9094", "localhost:9095", "localhost:9096"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "order-service" });

const run = async () => {
    try {
        await producer.connect();
        await consumer.connect();

        await consumer.subscribe({ topic: "payment-successful", fromBeginning: true });


        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const value = message.value.toString();
                console.log(`Received message: ${value} from topic: ${topic}`);

                const { userId,cart } = JSON.parse(value);

                //TODO: create order on DB
                const dummyOderId = "123456789"; // Simulating order creation
                console.log(`Creating order for user ${userId} with order ID ${dummyOderId}`);
                await producer.send({
                    topic: "order-successful",
                    messages: [
                        {
                            value: JSON.stringify({
                                userId,
                                orderId: dummyOderId,
                            }),
                        },
                    ],
                });
                
                // Here you can process the message and perform analytics operations
                // For example, you could save the data to a database or perform calculations
            },
        })
    } catch (error) {
        console.error("Error connecting to Kafka Producer:", error);
        
    }
}

run()