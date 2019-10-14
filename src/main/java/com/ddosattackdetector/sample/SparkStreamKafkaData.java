package com.ddosattackdetector.sample;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

public class SparkStreamKafkaData
{
    private static final Logger logger = LoggerFactory.getLogger(SparkStreamKafkaData.class);

    public static void main(String[] args) throws InterruptedException
    {
        System.setProperty("hadoop.home.dir","C:\\Tools\\hadoop-common-2.2.0-bin-master");
        long duration = Long.parseLong(args[0]);

        final int noOfRequests = Integer.parseInt(args[1]);

        long sleepDuration = Long.parseLong(args[2]);

        if(duration == 0)
        {
            duration = 5000;
        }

        SparkConf sparkConf = new SparkConf().setAppName("DetectDDOSAttack").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaStreamingContext jscx = new JavaStreamingContext(jsc, new Duration(duration));
        Map<String,String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers","localhost:9092");
        kafkaParams.put("group.id","ddos-group");
        kafkaParams.put("auto.offset.reset", "smallest");
        Set<String> topicName = Collections.singleton("detectDDOS");


        JavaPairInputDStream<String, String> kafkaSparkPairInputDStream = KafkaUtils
                .createDirectStream(jscx, String.class, String.class,
                        StringDecoder.class, StringDecoder.class, kafkaParams,
                        topicName);

        logger.info("Get all ip addresses");

        JavaDStream<String> ipAddresses =
                kafkaSparkPairInputDStream.flatMap(new FlatMapFunction<Tuple2<String, String>, String>()
        {
            @Override
            public Iterator<String> call (Tuple2<String, String> tuple2) throws Exception
            {
                String logLine = tuple2._2();
                String[] data = logLine.split(" ");
                logger.info("Remote host = {}", data[0]);
                return Arrays.asList(data[0]).iterator();
            }
        });

        logger.info("Assign a counter to each ipaddress");

        JavaPairDStream<String, Integer> ipAddressMap =
                ipAddresses.mapToPair(new PairFunction<String, String, Integer>()
        {
            @Override
            public Tuple2<String, Integer> call(String ipaddress) throws Exception
            {
                return new Tuple2<>(ipaddress, 1);
            }
        });

        logger.info("count unique ip addresses");

        JavaPairDStream<String, Integer> ipAddressCount = ipAddressMap.reduceByKey(new Function2<Integer, Integer, Integer>()
        {
            @Override
            public Integer call (Integer v1, Integer v2) throws Exception
            {
                return v1+v2;
            }
        });

        logger.info("Filter ip addresses which made more than noOfRequests requests");

        JavaPairDStream<String, Integer> flaggedIpAddresses = ipAddressCount.filter(new Function<Tuple2<String, Integer>, Boolean>()
        {
            @Override
            public Boolean call (Tuple2<String, Integer> v1) throws Exception
            {
                return v1._2() > noOfRequests;
            }
        });

        logger.info("Starting to stream the data");

        flaggedIpAddresses.dstream().saveAsTextFiles("C:\\temp\\DDOSAttackFound","");

        jscx.start();
        Thread.sleep(sleepDuration);
        jscx.stop();
        logger.info("Done streaming, now ending the program");
        //jscx.awaitTermination();

    }
}