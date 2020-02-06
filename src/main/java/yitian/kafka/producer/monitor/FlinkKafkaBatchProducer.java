package yitian.kafka.producer.monitor;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;
import yitian.kafka.producer.tools.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FlinkKafkaBatchProducer {
    private static Properties kafkaProps;
    private static String txtFile = "D:\\Intellij Workspaces\\kafka-producer-yitian\\src\\main\\resources\\A_Tale_of_Two_City.txt";
    private static String sendMessageSpeedFile = "D:\\send_message_speed.txt";
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
        File file = new File(txtFile);
        long startTime = System.currentTimeMillis();
        long messageCount = 0;

        // send message to kafka
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                if (!line.equals("")) {
                    // method - 1, statistic send messages speed per 5 seconds
//                    long nowTime = System.currentTimeMillis();
//                    long internalTime = nowTime - startTime;
//                    if (internalTime >= 5000) { // 5s
//                        int messageAveraeSpeed = (int) (messageCount / 5);
//                        System.out.println("InternalTime is: " + internalTime + " ms");
//                        System.out.println("Message average speed is: " + messageAveraeSpeed + " with internal time: " + internalTime + " has messages count: " + messageCount);
//                        FileUtils.writeToFile(sendMessageSpeedFile, " : " + messageAveraeSpeed);
//                        startTime = nowTime;
//                    }
                    // send messages
                    ProducerRecord<String, String> msg = new ProducerRecord<String, String>(KafkaProperties.KAFKA_TOPIC, line);
                    Thread.sleep(sleepSeconds);
                    producer.send(msg);
                    messageCount += 1;
                    System.out.println("Sent message " + messageCount + " : " + line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            bufferedReader.close();
        }
        System.out.println("Message sent successfully!");

        // method - 2
        long nowTime = System.currentTimeMillis();
        long internalTime = nowTime - startTime; // calculating the time (seconds)
        double internalSeconds = internalTime / 1000.0;
        System.out.println("InternalTime is: " + internalSeconds);
        int averageMessageSpeed = (int) (messageCount / internalSeconds);
        System.out.println("Origin Message speed is: " + averageMessageSpeed + " with messageCount: " + messageCount + " and internal time: " + internalSeconds);
//        Origin Message speed is: 20744 with messageCount: 3009 and internal time: 0.145
        FileUtils.writeToFile(sendMessageSpeedFile, " : " + averageMessageSpeed);

        // show topic info
//        List<PartitionInfo> partitionInfoList = new ArrayList<>();
////        partitionInfoList = producer.partitionsFor(KafkaProperties.KAFKA_TOPIC);
////        for (PartitionInfo info : partitionInfoList) {
////            System.out.println(info);
////        }
////        System.out.println("Show topic info end.");

        // closing producer
        producer.close();
    }

    private static void sendMessageSpeedMonitor() throws IOException, InterruptedException {
        for (int i = 0; i < 10; i++) {
            System.out.println("---------------------------RUNNING " + i + " TIMES---------------------------");
            produceMessagesLimitation();
            Thread.sleep(1000 * 2);
        }
    }

    // test function
    public static void main(String[] args) throws IOException, InterruptedException {
        // initialize kafka
        initKafka();

//        sendMessageSpeedMonitor();
        produceMessagesLimitation();
    }
}
