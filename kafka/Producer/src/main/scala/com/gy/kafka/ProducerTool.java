package com.gy.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
/**
 * Created by Administrator on 2019/12/23 0023.
 */
public class ProducerTool {
    //Kafka配置文件
    private static String brokerList="10.193.11.13:9092,10.193.11.14:9092";
    public static final String TOPIC_NAME = "test";
    public static final int producerNum=20;//实例池大小
    //阻塞队列实现生产者实例池,获取连接作出队操作，归还连接作入队操作
    public static BlockingQueue<KafkaProducer<String, String>> queue=new LinkedBlockingQueue<KafkaProducer<String, String>>(producerNum);
    //初始化producer实例池
    static {
        for (int i = 0; i <producerNum ; i++) {
            KafkaProducer<String, String> kafkaProducer = getProducer();
            queue.add(kafkaProducer);
        }
    }

    public static void sendMessage(String msg){
        ProducerRecord record = new ProducerRecord(TOPIC_NAME, msg);
        try {
            KafkaProducer<String, String> kafkaProducer =queue.take();//从实例池获取连接,没有空闲连接则阻塞等待
            kafkaProducer.send(record);
            queue.put(kafkaProducer);//归还kafka连接到连接池队列
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static Properties initConfig(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.RETRIES_CONFIG,5);
        props.put(ProducerConfig.ACKS_CONFIG,"1");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("security.protocol", "SASL_PLAINTEXT");

        return props;
    }
    public static KafkaProducer  getProducer(){
        Properties props = initConfig();
        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System. setProperty("java.security.krb5.conf", "/opt/goldenBoxConf/krb5.conf" );
        System.setProperty("java.security.auth.login.config","/opt/goldenBoxConf/jaas.conf");
        KafkaProducer producer = new KafkaProducer<String , String>(props);
        return producer;
    }
}
