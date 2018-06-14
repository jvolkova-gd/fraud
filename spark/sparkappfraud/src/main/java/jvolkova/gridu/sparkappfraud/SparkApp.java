package main.java.jvolkova.gridu.sparkappfraud;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.None;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class SparkApp {

    public static Properties prop = prop();
    public SparkConf conf = null;


    final Integer windowSec = Integer.parseInt(getAnalytic("window"));
    final Integer slideSec = Integer.parseInt(getAnalytic("slide"));
    final Double nullViews = Double.parseDouble(getAnalytic("null.views"));
    final Integer maxEvents = Integer.parseInt(getAnalytic("maxevents.perip"));
    final Integer maxCat = Integer.parseInt(getAnalytic("maxcat.perip"));
    final Double clicksToViewsMax = Double.parseDouble(getAnalytic("clicksToViewsMax"));
    final Integer ttl = Integer.parseInt(prop.getProperty("cassandra.output.ttl"));

    SparkApp() {
        // init SparkApp (SparkConf)

        conf = new SparkConf()
                .set("spark.cassandra.connection.host", prop.getProperty("cassandra.host"))
                .set("spark.cassandra.output.ttl", prop.getProperty("cassandra.output.ttl"));

    }
    public JavaInputDStream<ConsumerRecord<String, String>> getStream(JavaStreamingContext ssc, String groupKey){

        final Collection<String> topics = Arrays.asList(SparkApp.prop.getProperty("kf.topics").split(","));

        Map<String, Object> kafkaParams = getDriverKafkaConf();
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", prop.getProperty("kf.group." + groupKey + ".id"));

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams));
        return stream;
    }
    public static Properties prop(){
        // return access to properties file
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(
                    "conf/spark.properties"));
        }
        catch (IOException e){
            e.printStackTrace();
        }
        return prop;
    }
    public String getAnalytic(String field){
        // return analytic parameters by key
        String fieldValue = null;
        String analyticPrefix = "analytic.";
        fieldValue = prop.getProperty(analyticPrefix+field);
        return fieldValue;
    }

    public Map<String, Object> getDriverKafkaConf() {
        // return special properties for kafka Streams
        String configPrefix = "kf.";
        Map<String, Object> params = new HashMap<>();
        Enumeration<?> e = prop.propertyNames();

        while (e.hasMoreElements()) {

            String key = (String) e.nextElement();
            if (key.startsWith(configPrefix) && key != "kf.topics" && !key.startsWith("kf.group.id")) {
                params.put(key.split(configPrefix)[1], prop.getProperty(key));
            }
        }
        return params;
    }
}
