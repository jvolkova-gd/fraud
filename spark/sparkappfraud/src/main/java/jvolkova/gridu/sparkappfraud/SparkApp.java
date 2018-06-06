package jvolkova.gridu.sparkappfraud;

import org.apache.spark.SparkConf;
import scala.None;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class SparkApp {

    public static Properties prop = prop();
    public SparkConf conf = null;

    SparkApp() {
        // init SparkApp (SparkConf)

        conf = new SparkConf()
                .set("spark.cassandra.connection.host", prop.getProperty("cassandra.host"))
                .set("spark.cassandra.output.ttl", prop.getProperty("cassandra.output.ttl"));
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
            if (key.startsWith(configPrefix) && key != "kf.topics") {
                params.put(key.split(configPrefix)[1], prop.getProperty(key));
            }
        }
        return params;
    }
}
