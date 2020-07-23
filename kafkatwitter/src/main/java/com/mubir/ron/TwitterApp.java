package com.mubir.ron;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.google.common.collect.Lists;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterApp {
    Logger logger = LoggerFactory.getLogger(TwitterApp.class.getName());
    String consumerKey = "YOMJ8pzDq7kF6AY5RXDvhhWPl";
    String consumerSecret = "OdGciIFv0A4US8zhHAsZpNghmh9bF2b9aaK9bO23pqpCvPwny2";
    String token = "753132666980098048-PyfsiEqaIIvASoTx5h6GuVyknNdKhyf";
    String secret = "H8NNs5HTOv9APfA8DEj4NdkldRRRbZISvpnPxDv0eMeOr";

    public TwitterApp() {

    }

    public static void main(String[] args) {
        new TwitterApp().run();
    }

    public void run() {
        /**
         * Set up your blocking queues: Be sure to size these properly based on expected
         * TPS of your stream
         */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        Client client = createTwitterClient(msgQueue);
        client.connect();
        KafkaProducer<String,String> producer = CreateProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from twitter...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");
        }));
        while (!client.isDone()) {
            String data = null;
            try {
                 data = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // TODO: handle exception
                e.printStackTrace();
                client.stop();
            }
            if (data != null) {
                logger.info(data);
                producer.send(new ProducerRecord<String,String>("twitter_app", null, data), new Callback(){
                
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception != null)
                        {
                            logger.error("!!! EOORr encounter ",exception);
                        }
                    }
                });
            }
        }
    }

    private KafkaProducer<String, String> CreateProducer() {
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // create safe Producer
        //enablse idempotence
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        // tries for 2147483647 times 
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        // 5 parallel request
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        

        // high throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size

        KafkaProducer<String,String> producer= new KafkaProducer<>(properties);
        return producer;
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /**
         * Declare the host you want to connect to, the endpoint, and authentication
         * (basic auth or oauth)
         */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms

        List<String> terms = Lists.newArrayList("trump");

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder().name("Client-01") // optional: mainly for the logs
                .hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client client = builder.build();
        // Attempts to establish a connection.
        return client;

    }
}