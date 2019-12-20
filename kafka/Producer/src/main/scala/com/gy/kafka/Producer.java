package com.gy.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by Administrator on 2019/11/13 0013.
 */
public class Producer {
    private String brokerList="";
    private String topic="";
    private KafkaProducer<String, String> producer=null;
    public Producer(String brokList, String topic){
        this.brokerList = brokList;
        this.topic = topic;
        initProducer();
    }



    public  Properties initConfig(){
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
    public void  initProducer(){
        Properties props = initConfig();
        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System. setProperty("java.security.krb5.conf", "/opt/goldenBoxConf/krb5.conf" );
        System.setProperty("java.security.auth.login.config","/opt/goldenBoxConf/jaas.conf");
        producer = new KafkaProducer<String , String>(props);
    }







    public boolean sendMessage(String value){
        ProducerRecord record = new ProducerRecord<String , String>(topic, value);
        try{
            producer.send(record);
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

}
