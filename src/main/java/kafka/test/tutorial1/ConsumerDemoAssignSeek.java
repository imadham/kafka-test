package kafka.test.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {
        System.out.println("Consumer");

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        String boostrapServer = "127.0.0.1:9092";
        String groupId = "my_application_consumer";
        String imad_topic_2 = "imad_topic2";
        long offsetToReadFrom = 15L;

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //earliest latest none
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are mostly used to replay data or fetch a specific message

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(imad_topic_2, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessages = 5;
        boolean keepOnReading = true;
        int numberOfMessagesRead = 0;

        while (keepOnReading)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records)
            {
                numberOfMessagesRead++;
                logger.info("Key:"+record.key() +" value:"+record.value());
                logger.info("Partition:" + record.partition() +" offset:"+record.offset());
                if(numberOfMessagesRead >= numberOfMessages)
                    keepOnReading=false;
                break;
            }
        }
    }
}
