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
public class FlinkKafkaProducerFromTxt {
    private static Properties kafkaProps;
    private static String topic = "zk-topic";
    private static String topicFriend = "friends";
    private static String topicTime = "loginTime";
    private static String topicDevice = "device";
    private static String topicSource = "source";
//    private static String txtFile = "/home/ly/kafka-producer-yitian/src/main/resources/A_Tale_of_Two_City.txt";
    private static String txtFile = "/opt/data/data.csv";
//    private static String brokerURL = "192.168.209.137:9092";
    private static String brokerURL = "192.168.17.144:9092";
//    private static int field1 = 2;
//    private static int field2 = 8;

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
            String line2 = null;
            String friend = null;
            String loginTime = null;
            String device = null;
            String source = null;
            while ((line = bufferedReader.readLine()) != null) {
//            if ((line = bufferedReader.readLine()) != null) {
                if (!line.equals("")) {
//                    line = bufferedReader.readLine();
//                    line = bufferedReader.readLine();
                    ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, line);
                    //select field
                    line2 = selectField(line);
                    String[] select = line2.split(",");
                    if (line2 == null || line2 == "" || select.length < 9) continue;
                    friend = select[2] + "," + select[3];
                    loginTime = select[2] + "," + select[8];
                    device = select[2] + "," + select[4];
                    source = select[3] + "," + select[1];
                    ProducerRecord<String, String> msgFriend = new ProducerRecord<String, String>(topicFriend, friend);
                    ProducerRecord<String, String> msgTime = new ProducerRecord<String, String>(topicTime, loginTime);
                    ProducerRecord<String, String> msgDevice = new ProducerRecord<String, String>(topicDevice, device);
                    ProducerRecord<String, String> msgSource = new ProducerRecord<String, String>(topicSource, source);
                    final String finalLine = line;
                    final String finalLine2 = friend;
                    final String finalLine3 = loginTime;
                    final String finalLine4 = device;
                    final String finalLine5 = source;
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
                    producer.send(msgFriend, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                e.printStackTrace();
                            } else {
                                System.out.println("Sent message: " + finalLine2);
                            }
                        }
                    });
                    producer.send(msgTime, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                e.printStackTrace();
                            } else {
                                System.out.println("Sent message: " + finalLine3);
                            }
                        }
                    });
                    producer.send(msgDevice, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                e.printStackTrace();
                            } else {
                                System.out.println("Sent message: " + finalLine4);
                            }
                        }
                    });
                    producer.send(msgSource, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                e.printStackTrace();
                            } else {
                                System.out.println("Sent message: " + finalLine5);
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

    public static String selectField(String line) {
//        String tempStr = line;
        int count1 = 0;
        int count2 = 0;
        int begin = 0;
        int end = 0;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == ',') {
                count1++;
                if (count1 == 6) {
                    begin = i + 1;
                    break;
                }
            }
        }
        for (int i = line.lastIndexOf(","); i >= 0; i--) {
            if (line.charAt(i) == ',') {
                count2++;
                if (count2 == 4) {
                    end = i;
                    break;
                }
            }
        }
        if (begin > 0 && end > 0 && begin <= end) {
            String replaceStr = line.substring(begin, end).replace(',', '，');
            return line.substring(0, begin) + replaceStr + line.substring(end, line.length());
        } else {
            return "";
        }
//        line = new StringBuffer().append(line.substring(0,begin)).append(replaceStr).append(line.substring(end , line.length())).toString();
//        String[] select = line.split(",");
//        return select[field1] + select[field2];
//        return new StringBuffer().append(select[field1]).append(select[field2]).toString();
    }

    // test function
    public static void main(String[] args) throws IOException, InterruptedException {

        initKafka();

        produceMessagesFromTxt();
    }
}
