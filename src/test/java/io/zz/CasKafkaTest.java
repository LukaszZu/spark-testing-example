/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.zz;

import com.datastax.spark.connector.embedded.EmbeddedKafka;
import com.datastax.spark.connector.embedded.KafkaProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author zulk
 */
public class CasKafkaTest {

    private EmbeddedKafka embeddedKafka;
    private JavaStreamingContext jsc;
    private ObjectMapper mapper;
    private Set<String> topicsSet;
    private Map<String, String> kafkaParams;

    @Before
    public void before() {
        mapper = new ObjectMapper();
        embeddedKafka = new EmbeddedKafka();
        SparkConf sparkConf = new SparkConf(true).setMaster("local[*]")
                .setAppName("testApp");
        jsc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        
        embeddedKafka.createTopic("test", 1, 1);
        String brokers = embeddedKafka.server().socketServer().host()+":"+embeddedKafka.server().socketServer().port();
        topicsSet = new HashSet<>(Arrays.asList("test".split(",")));
        kafkaParams = new HashMap<>();  
        kafkaParams.put("metadata.broker.list", brokers);
    }

    @Test
    public void test() throws InterruptedException, JsonProcessingException {
        JavaPairInputDStream<String, String> createDirectStream = KafkaUtils.createDirectStream(jsc,
                String.class, String.class, StringDecoder.class, StringDecoder.class,
                kafkaParams, topicsSet);
        
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(embeddedKafka.producerConfig());
                
        kafkaProducer.send("test", TestMessage.class.getName(), mapper.writeValueAsString(new TestMessage("name", "value")));
        
        createDirectStream.foreachRDD(rdd -> rdd.foreachPartition( f -> f.forEachRemaining(t -> {
            System.out.println(t._1+":"+t._2);
        })));
        
        jsc.start();
        jsc.awaitTerminationOrTimeout(5000);
    }

    @After
    public void after() {
        jsc.stop();
        embeddedKafka.shutdown();
    }

}
