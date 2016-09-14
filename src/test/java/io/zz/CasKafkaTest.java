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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import kafka.serializer.StringDecoder;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import static org.apache.spark.sql.Encoders.bean;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.collection.mutable.WrappedArray;

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
        for (int i = 0; i < 100; i++) {
            TestMessage testMessage = new TestMessage("name", "1,2,3,4,5,6,7,8");
            testMessage.addMessage(new InMessage("A"))
                    .addMessage(new InMessage("B"));
            kafkaProducer.send("test", TestMessage.class.getName(), mapper
                    .writeValueAsString(testMessage));
        }
        

        createDirectStream.print();
        
        
        createDirectStream.transform((d,t) -> {
            ObjectMapper om = new ObjectMapper();
            
            JavaRDD<TestMessage> map = d.map(tp -> om.readValue(tp._2,TestMessage.class));
            map.foreach(f -> System.out.println(f));
            System.out.println("---------------------------------------------------------");
            Dataset<TestMessage> ds = SparkSession.builder().getOrCreate().createDataset(map.rdd(), bean(TestMessage.class));
            return ds.toJavaRDD();
        }).foreachRDD(f -> System.out.print(f));
        
        createDirectStream.foreachRDD(rdd -> rdd.foreachPartition(f -> {
            ArrayList<InMessage> arrayList = new ArrayList<>();
            ArrayList<TestMessage> tm = new ArrayList<>();
            ObjectMapper om = new ObjectMapper();

            f.forEachRemaining(t -> {

                try {
                    TestMessage readValue = om
                            .readValue(t._2, TestMessage.class);
                    arrayList.addAll(readValue.getInMessage());
                    tm.add(readValue);
                } catch (IOException ex) {
                    Logger.getLogger(CasKafkaTest.class.getName())
                            .log(Level.SEVERE, null, ex);
                }
            });
            JavaSparkContext lsc = JavaSparkContext
                    .fromSparkContext(SparkContext.getOrCreate());
            LocalTime rdd1 = LocalTime.now();
            JavaRDD<InMessage> parallelize = lsc.parallelize(arrayList);
            List<Partition> partitions = parallelize.groupBy(fn -> fn.getName()).partitions();
            LocalTime rdd2 = LocalTime.now();
            System.out
                    .println("RDD: " + ChronoUnit.MILLIS.between(rdd1, rdd2) + " ->" + partitions.size());

            SparkSession ss = SparkSession.builder().getOrCreate();
            LocalTime df1 = LocalTime.now();
            Dataset<InMessage> ds = ss.createDataset(arrayList, Encoders.bean(InMessage.class));
            Dataset<Row> countDF = ds.groupBy(ds.col("name")).count();
            LocalTime df2 = LocalTime.now();
            System.out.println("DF: " + ChronoUnit.MILLIS.between(df1, df2) + " ->" + countDF);
            
            Dataset<TestMessage> fds = ss.createDataset(tm, Encoders.bean(TestMessage.class));
            fds.printSchema();
            ss.udf().register("u_join", (WrappedArray s,String d) -> s.mkString(d),DataTypes.StringType);
            fds.select(col("value"),explode(col("inMessage")).as("inMessage")).select("value","inMessage.name","inMessage.name2").show();
            fds.select(col("name"),col("value"),callUDF("u_join",col("inMessage.name"),lit("-")).as("dupa3")).show();
            fds.selectExpr("u_join(inMessage.name,'x') as dupa").show();
            fds.select(col("name"),explode(split(col("value"), ","))).show();
            
            Column[] cols = Stream.of(fds.columns()).map(c -> col(c)).collect(Collectors.toList()).toArray(new Column[]{});
            fds.select(cols).show();
            
        }));

        jsc.start();
//        jsc.awaitTermination();
        jsc.awaitTerminationOrTimeout(2000);
    }

    @After
    public void after() {
        jsc.stop();
        embeddedKafka.shutdown();
    }

}
