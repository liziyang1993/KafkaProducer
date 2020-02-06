package yitian.kafka.producer.backup;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Flink kafka producer from txt backup
 * Time: 2018-09-29
 */
public class FlinkKafkaProducerFromAllTxt {
    private static Properties kafkaProps;
    private static String topic = "zk-topic";
//    private static String txtFile = "/home/ly/kafka-producer-yitian/src/main/resources/A_Tale_of_Two_City.txt";
    private static String txtFile = "/opt/data/week9.csv";
//    private static String brokerURL = "192.168.209.137:9092";
    private static String brokerURL = "192.168.17.144:9092";

    private static void initKafka() {
        kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", brokerURL);
        kafkaProps.put("metadata.broker.list", brokerURL);
        kafkaProps.put("serializer.class", "kafka.serializer.StringEncoder");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("request.required.acks", "1");
    }

    private static void produceMessagesFromTxt() throws IOException, InterruptedException {
        Producer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);
        File file = new File(txtFile);

        // send message to kafka
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                    if (!line.equals("")) {
                        ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, line);
                        final String finalLine = line;
//                    Thread.sleep(10);
                        producer.send(msg, new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                if (e != null) {
                                    e.printStackTrace();
                                } else {
                                    System.out.println("Sent message: " + finalLine);
                                }
                            }
                        });
                    }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            bufferedReader.close();
        }
        System.out.println("Message sent successfully!");

        // show topic info
        List<PartitionInfo> partitionInfoList = new ArrayList<>();
        partitionInfoList = producer.partitionsFor(topic);
        for (PartitionInfo info : partitionInfoList) {
            System.out.println(info);
        }
        System.out.println("Show topic info end.");

        // closing producer
        producer.close();
    }

    // test function
    public static void main(String[] args) throws IOException, InterruptedException {

        initKafka();

        produceMessagesFromTxt();
    }
}
