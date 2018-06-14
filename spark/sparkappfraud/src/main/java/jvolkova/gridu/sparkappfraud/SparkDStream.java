package main.java.jvolkova.gridu.sparkappfraud;
import java.io.Serializable;
import java.util.*;
import java.lang.InterruptedException;
import java.util.Arrays;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.streaming.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;
import scala.Tuple2;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import main.java.jvolkova.gridu.sparkappfraud.dataclasses.FullDataRow;
import main.java.jvolkova.gridu.sparkappfraud.dataclasses.BotDataRow;
import main.java.jvolkova.gridu.sparkappfraud.dataclasses.CtrDataRow;

public class SparkDStream {

    public static void main(String[] args) throws InterruptedException {

        SparkApp app = new SparkApp();

        // analytic params
        final Integer maxEvents = Integer.parseInt(app.getAnalytic("maxevents.perip"));
        final Integer maxCat = Integer.parseInt(app.getAnalytic("maxcat.perip"));
        final Double clicksToViewsMax = Double.parseDouble(app.getAnalytic("clicksToViewsMax"));
        final Double nullViews = Double.parseDouble(app.getAnalytic("null.views"));
        final Integer ttl= Integer.parseInt(app.prop.getProperty("cassandra.output.ttl"));
        //window params
        final Integer windowSec = Integer.parseInt(app.getAnalytic("window"));
        final Integer slideSec = Integer.parseInt(app.getAnalytic("slide"));

        //cassandra params
        final String keyspace = app.prop.getProperty("cassandra.keyspace.dstream");
        final String botTable = app.prop.getProperty("cassandra.table.bots");
        final String ctrTable = app.prop.getProperty("cassandra.table.ctr");

        JavaStreamingContext ssc = new JavaStreamingContext(app.conf,
                new Duration(1));

        JavaInputDStream<ConsumerRecord<String, String>> messages = app.getStream(ssc, "ds");

        ObjectMapper map = new ObjectMapper();

        // Get the lines, split them into words, count the words and print
        JavaPairDStream<String, FullDataRow> lines = messages.mapToPair(s -> new Tuple2<>(s.key(),
                new FullDataRow(s.key(),
                s.topic().equals("clicks") ? 1.0 : 0,
                s.topic().equals("views") ? 1.0 : 0,
                        map.readTree(s.value()).get("category_id").asText())
        )).reduceByKeyAndWindow((a, b) -> new FullDataRow(a.getIp(), a.getClicks()+b.getClicks(),
                a.getViews()+b.getViews(), a.joinCategoriesIds(b.getCategoriesIds())),
                Durations.seconds(windowSec), Durations.seconds(slideSec)).mapValues(
                        s -> new FullDataRow(s.getIp(), s.getClicks(), s.getViews() > 0 ? s.getViews() : nullViews,
                                s.getCategoriesIds(), s.getClicks()/s.getViews()));

        JavaDStream<CtrDataRow> ctr = lines.map(s -> (new CtrDataRow(s._1, s._2.getCtr())));

        Map<String, String> ctrColumnNameMappings = new HashMap<>();
        ctrColumnNameMappings.put("ip", "ip");
        ctrColumnNameMappings.put("ctr", "ctr");

        ctr.foreachRDD((rdd, time) -> {
            rdd.saveAsTextFile("/log/ctr");
                });
        javaFunctions(ctr).writerBuilder(keyspace, ctrTable,
                CassandraJavaUtil.mapToRow(CtrDataRow.class, ctrColumnNameMappings)).saveToCassandra();

        JavaDStream<FullDataRow> lines_with_bots = lines.map(
                s -> new FullDataRow(s._2.getClicks() + s._2.getViews() > maxEvents
                        || s._2.getCtr() > clicksToViewsMax  || s._2.getCategoriesIds().size() > maxCat));

        JavaDStream<BotDataRow> bots = lines_with_bots.filter(s -> s.getBot()).map(s -> new BotDataRow(s.getIp()));

        Map<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("ip", "ip");

        javaFunctions(bots).writerBuilder(keyspace, botTable,
                CassandraJavaUtil.mapToRow(BotDataRow.class, columnNameMappings)).saveToCassandra();

        ssc.checkpoint("/spark-system/ds");
        ssc.start();
        ssc.awaitTermination();
    }

}

