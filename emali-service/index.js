import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: "email-service",
  brokers: ["localhost:9094", "localhost:9095", "localhost:9096"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "email-service" });

const run = async () => {
    try {
        await producer.connect();
        await consumer.connect();

        await consumer.subscribe({ topic: "order-successful", fromBeginning: true });


        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const value = message.value.toString();
                console.log(`Received message: ${value} from topic: ${topic}`);

                const { userId, orderId } = JSON.parse(value);

                //TODO: Send email TO user
                const dummyEmailId = "123456789DASD"; // Simulating order creation

                console.log(`Sending email to user ${userId} for order ${dummyEmailId}`);
                await producer.send({
                    topic: "email-successful",
                    messages: [
                        {
                            value: JSON.stringify({
                                userId,
                                emailId: dummyEmailId,
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