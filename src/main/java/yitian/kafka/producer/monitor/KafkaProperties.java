package yitian.kafka.producer.monitor;

import java.util.Properties;

public class KafkaProperties {

    public static String KAFKA_TOPIC = "zk-topic";
    public static String BROKER_NAME = "192.168.17.144";
    public static String BROKER_URL = "192.168.17.144:9092";
    public static String SERIALIZER_CLASS = "kafka.serializer.StringEncoder";
    public static String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static String REQUEST_ACKS = "1";
}
