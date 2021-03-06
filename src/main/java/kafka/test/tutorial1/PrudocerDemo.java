package kafka.test.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class PrudocerDemo {



    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(PrudocerDemo.class);
        System.out.println("hello world");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i=0;i<20;i++) {
        //create producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("imad_topic2", "hello world "+i);

        //send data - asynchrous
//        producer.send(producerRecord);


            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null) {
                        logger.info("recived new metadata.\nTopic:" + recordMetadata.topic() + "\nPartitions: " + recordMetadata.partition() + "" +
                                "\nOffset:" + recordMetadata.offset() + "\nTimestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("error", e);
                    }

                }
            });

            producer.flush(); // or .colse()

        }
    }
}
