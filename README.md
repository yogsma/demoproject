# DDOS Attack Detector - Demo Project

This is a sample project to ddos attack based on apache access log.

It contains two elements

1. Apacke Kafka
   - The main program reads a file from a directory and sends the data to Apache Kafka Producer topic. 
   - This program takes a parameter of source directory where it can read log file from.
   
2. Apache Spark
   - Spark streaming subscribes to Kafka Producer topic and processes the log data to find out if there was a DDOS attack.
   - This program takes three parameters duration for which log to process, count of requests to be considered for flagging
     duration to sleep during stream processing.
     
This project has been written using IntelliJ and gradle, would recommend executing through IDE rather than command line. 
