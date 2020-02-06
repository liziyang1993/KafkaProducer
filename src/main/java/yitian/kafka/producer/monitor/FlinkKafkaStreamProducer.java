package yitian.kafka.producer.monitor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import yitian.kafka.producer.tools.FileReader;
import yitian.kafka.producer.tools.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FlinkKafkaStreamProducer {
    private static Properties kafkaProps;
    private static String sendMessageSpeedFile = "D:\\send_message_speed.txt";
    private static final String TXT_FILE = "/A_Tale_of_Two_City.txt";
    private static FileReader fileReader = null;
    private static long messageCount = 0;
    private static int sleepSeconds = 0; // unit is ms

    private static void initKafka() {
        kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", KafkaProperties.BROKER_URL);
        kafkaProps.put("metadata.broker.list", KafkaProperties.BROKER_URL);
        kafkaProps.put("serializer.class", KafkaProperties.SERIALIZER_CLASS);
        kafkaProps.put("key.serializer", KafkaProperties.KEY_SERIALIZER);
        kafkaProps.put("value.serializer", KafkaProperties.VALUE_SERIALIZER);
        kafkaProps.put("request.required.acks", KafkaProperties.REQUEST_ACKS);
    }

    private static void produceMessagesLimitation() throws IOException, InterruptedException {
        Producer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);
        fileReader = new FileReader(TXT_FILE);
        long startTime = System.currentTimeMillis();

        try {
            while (true) {
                String line = fileReader.nextLine();

                ProducerRecord<String, String> msg = new ProducerRecord<String, String>(KafkaProperties.KAFKA_TOPIC, line);
                Thread.sleep(sleepSeconds);
                producer.send(msg);
                messageCount += 1;
                System.out.println("Sent message " + messageCount + " : " + line);

                // statistics
                long nowTime = System.currentTimeMillis();
                long internalTime = nowTime - startTime; // calculating the time (seconds)
                if (internalTime >= 10 * 1000) {
                    double internalSeconds = internalTime / 1000.0;
                    System.out.println("InternalTime is: " + internalSeconds);
                    int averageMessageSpeed = (int) (messageCount / internalSeconds);
                    System.out.println("Origin Message speed is: " + averageMessageSpeed + " with messageCount: " + messageCount + " and internal time: " + internalSeconds);
                    //        Origin Message speed is: 20744 with messageCount: 3009 and internal time: 0.145
                    FileUtils.writeToFile(sendMessageSpeedFile, " : " + averageMessageSpeed);

                    startTime = nowTime;
                    messageCount = 0;
                }
            }
        } finally {
            // closing producer
            producer.close();
        }

    }

    // test function
    public static void main(String[] args) throws IOException, InterruptedException {
        // initialize kafka
        initKafka();
        produceMessagesLimitation();
    }
}
