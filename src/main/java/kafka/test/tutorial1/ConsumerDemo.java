package kafka.test.tutorial1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        System.out.println("Consumer");

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String boostrapServer = "127.0.0.1:9092";
        String groupId = "my_application_consumer";
        String imad_topic_2 = "imad_topic2";

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //earliest latest none
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //create consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe to topic


        consumer.subscribe(Arrays.asList(imad_topic_2));//Arrays.asList("first", "second", "...");


        while (true)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records)
            {
                logger.info("Key:"+record.key() +" value:"+record.value());
                logger.info("Partition:" + record.partition() +" offset:"+record.offset());
            }
        }
    }
}
