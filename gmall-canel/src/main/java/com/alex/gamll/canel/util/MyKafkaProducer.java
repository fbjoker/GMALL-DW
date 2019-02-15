package com.alex.gamll.canel.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyKafkaProducer {

    public static KafkaProducer<String, String> producer=null;



    public  static KafkaProducer<String, String> creatProducer(){

        Properties prop = new Properties();

        prop.setProperty("bootstrap.servers","hadoop102:9092");
        prop.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("acks","1");


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        return  producer;

    }


    public  static  void  send(String topic,String val){
        if(producer==null){
            producer=creatProducer();
        }

        producer.send(new ProducerRecord<String, String>(topic,val));


    }


}
