package edu.supmti.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EventProducer {

    public static void main(String[] args) throws Exception {

        // Vérifier que le topic est fourni comme argument
        if (args.length == 0) {
            System.out.println("Entrer le nom du topic");
            return;
        }

        String topicName = args[0];

        // Config du producer
        Properties props = new Properties();

        // Adresse du broker Kafka
        props.put("bootstrap.servers", "localhost:9092");
        // Acquittement
        props.put("acks", "all");
        // Pas de retries pour simplifier
        props.put("retries", 0);
        // Taille du batch
        props.put("batch.size", 16384);
        // Mémoire buffer
        props.put("buffer.memory", 33554432);
        // Sérializers
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        // envoyer 10 messages simples clé=valeur
        for (int i = 0; i < 10; i++) {
            String key = Integer.toString(i);
            String value = Integer.toString(i);
            producer.send(new ProducerRecord<String, String>(topicName, key, value));
        }

        System.out.println("Messages envoyés avec succès");

        producer.close();
    }
}
