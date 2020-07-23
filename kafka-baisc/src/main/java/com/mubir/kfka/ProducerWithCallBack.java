package com.mubir.kfka;

import java.util.Properties;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithCallBack {
    public static void main(String[] args)
    {
        //System.out.println( "Hello World!" );
        final Logger logger = LoggerFactory.getLogger(ProducerWithCallBack.class);
        String bootStrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);
        for(int i=0;i<9;i++){
            String topic = "first_topic";
            String value="form vscode "+Integer.toString(i);
            //String key =Integer.toString(i);
        ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic,value);
            //logger.info(key);
        producer.send(record,new Callback(){
        
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(exception == null)
                {
                    logger.info("************************Metadata *************************\n"+
                    "Topic : "+metadata.topic()+
                    "\nPartition : "+metadata.partition()+
                    "\nOffset : "+metadata.offset()+
                    "\nTimeStamp :"+metadata.timestamp()+
                    "\n###################################################"
                    );
                }else
                {
                    logger.error("Error encounter",exception);
                }
            }
        });
        }
        producer.flush();
        producer.close();
        
    }
}