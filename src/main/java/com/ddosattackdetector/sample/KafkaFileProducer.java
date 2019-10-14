package com.ddosattackdetector.sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class KafkaFileProducer
{
    //private static final Logger logger = LoggerFactory.getLogger(KafkaFileProducer.class);
    public static void main(String[] args)
    {
        //logger.info("Starting to read the file...");
        final String fileName = "apache-access-log.txt";
        final String directory = args[0];
        if(directory == null || directory.isEmpty())
        {
            throw new RuntimeException("No source directory was passed");
        }
        //logger.info("Directory of the file is {}", directory);
        String topicName = "detectDDOS";

        final KafkaProducer<String, String> kafkaProducer;

        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("group.id","ddos-group");
        properties.put("client.id","KafkaFileProducer");
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer<String, String>(properties);

        int count = 0;
        String line;
        try(BufferedReader bufferedReader =
                    new BufferedReader(new FileReader(directory + fileName)))
        {
            while((line = bufferedReader.readLine()) != null)
            {
                count++;
                kafkaProducer.send(new ProducerRecord<>(topicName, Integer.toString(count),line));
            }
        }
        catch(IOException e)
        {
            throw new RuntimeException("File not found or not able to open");
        }

        kafkaProducer.close();

    }
}
