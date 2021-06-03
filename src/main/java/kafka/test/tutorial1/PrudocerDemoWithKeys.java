package kafka.test.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class PrudocerDemoWithKeys {



    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(PrudocerDemoWithKeys.class);
        System.out.println("hello world");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //safer producer

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");//keep ordering for kafka >2.0


        //high throughput  producer (at the expense of a bit latency and cpu usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");//ms
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        long time = System.currentTimeMillis();
        for (int i=0;i<500;i++) {
            String topic = "imad_topic2";
            String value = "hello "+ i;
            String key = "id_" + i;
        //create producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);

        //send data - asynchrous
//        producer.send(producerRecord);

            logger.info("Key \t\t\t\t\t" + key);

            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null) {
                        logger.info("recived new metadata.\nTopic:" + recordMetadata.topic() + "\nPartitions: " + recordMetadata.partition() + "" +
                                "\nOffset:" + recordMetadata.offset() + "\nTimestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("error", e);
                    }

                }
            }).get();//block asynch to make it synch, bad practise

            producer.flush(); // or .colse()

        }

        System.out.println(System.currentTimeMillis()-time);
    }
}
