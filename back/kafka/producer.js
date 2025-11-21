import { Kafka } from "kafkajs";

const kafka_cluster_address = process.env.KAFKA;

const kafka = new Kafka({
    clientId: "backend-express",
    brokers: [kafka_cluster_address],
});

export const producer = kafka.producer();

export const connectProducer = async () => {
    await producer.connect();
    console.log("Connection du producer à kafka effectuée");
};
