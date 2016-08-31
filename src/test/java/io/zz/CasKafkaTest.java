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
import java.io.IOException;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
        String brokers = embeddedKafka.server().socketServer().host() + ":" + embeddedKafka
                .server().socketServer().port();
        topicsSet = new HashSet<>(Arrays.asList("test".split(",")));
        kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
    }

    @Test
    public void test() throws InterruptedException, JsonProcessingException {
        JavaPairInputDStream<String, String> createDirectStream = KafkaUtils
                .createDirectStream(jsc,
                        String.class, String.class, StringDecoder.class, StringDecoder.class,
                        kafkaParams, topicsSet);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(embeddedKafka
                .producerConfig());
        for (int i = 0; i < 10000; i++) {
            TestMessage testMessage = new TestMessage("name", "value");
            testMessage.addMessage(new InMessage("A"))
                    .addMessage(new InMessage("B"));
            kafkaProducer.send("test", TestMessage.class.getName(), mapper
                    .writeValueAsString(testMessage));
        }
        
        
        createDirectStream.foreachRDD(rdd -> rdd.foreachPartition(f -> {
            ArrayList<InMessage> arrayList = new ArrayList<>();
            ObjectMapper om = new ObjectMapper();

            f.forEachRemaining(t -> {

                try {
                    TestMessage readValue = om
                            .readValue(t._2, TestMessage.class);
                    arrayList.addAll(readValue.getInMessage());
                } catch (IOException ex) {
                    Logger.getLogger(CasKafkaTest.class.getName())
                            .log(Level.SEVERE, null, ex);
                }
            });
            JavaSparkContext lsc = JavaSparkContext
                    .fromSparkContext(SparkContext.getOrCreate());
            LocalTime rdd1 = LocalTime.now();
            JavaRDD<InMessage> parallelize = lsc.parallelize(arrayList);
            Map<String, Long> countRDD = parallelize.groupBy(fn -> fn.getName()).countByKey();
            LocalTime rdd2 = LocalTime.now();
            System.out
                    .println("RDD: " + ChronoUnit.MILLIS.between(rdd1, rdd2) + " ->" + countRDD);

            SparkSession ss = SparkSession.builder().getOrCreate();
            LocalTime df1 = LocalTime.now();
            Dataset<InMessage> ds = ss.createDataset(arrayList, Encoders.bean(InMessage.class));
            Dataset<Row> countDF = ds.groupBy(ds.col("name")).count();
            LocalTime df2 = LocalTime.now();
            System.out.println("DF: " + ChronoUnit.MILLIS.between(df1, df2) + " ->" + countDF);
        }));

        jsc.start();
        jsc.awaitTermination();
//        jsc.awaitTerminationOrTimeout(5000);
    }

    @After
    public void after() {
        jsc.stop();
        embeddedKafka.shutdown();
    }

}
