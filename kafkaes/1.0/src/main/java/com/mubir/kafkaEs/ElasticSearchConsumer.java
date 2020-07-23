package com.mubir.kafkaEs;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import com.google.gson.JsonParser;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createConsumerClient() {
        /*
         * IF YOU USE LOCAL ELASTICSEARCH
         * 
         * 
         * String hostname = "localhost"; RestClientBuilder builder =
         * RestClient.builder(new HttpHost(hostname,9200,"http"));
         */

        String hostname = "twitter-elas-5551776745.ap-southeast-2.bonsaisearch.net"; // localhost or bonsai url
        String username = "wybqb328ej"; // needed only for bonsai
        String password = "9s8w2u3mo"; // needed only for bonsai

        // credentials provider help supply username and password for cloud service
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        // connects to hostname and port url
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);

        return client;

    }

    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweetJson) {
        // gson library
        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "es-consumer";
        // String topic = "twitter_app";

        // set consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;

    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createConsumerClient();
        // twitter index must be pre set in elastic other wise error will encounter;

        // String data = "{\"name\":\"Rony\"}";

        KafkaConsumer<String, String> consumer = createConsumer("twitter_app");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            Integer recordCount = records.count();
            logger.info("#####################  Received " + recordCount + " records  ############");
            for (ConsumerRecord<String, String> record : records) {
                try {
                    /**
                     * for keeping idemponet kafka generic id String id =
                     * recor.topic()+"_"+record.partition()+"_"+record.offset();
                     */
                    try {
                        String id = extractIdFromTweet(record.value());
                        IndexRequest indexRequet = new IndexRequest("twitter", "tweets", id).source(record.value(),
                            XContentType.JSON);
                    } catch (Exception e) {
                        //TODO: handle exception
                    }
                    
                    
                   // IndexResponse indexResponse = client.index(indexRequet, RequestOptions.DEFAULT);
                    // String id = indexResponse.getId();
                   // logger.info(indexResponse.getId());
                } catch (NullPointerException e) {
                    logger.warn(e.toString());
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // offset commiting funtionalitty
            
            logger.info("COmmittt offset...");
            consumer.commitAsync();
            logger.info(" offsets have been commited ");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        //client.close();

    }

}