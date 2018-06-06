package jvolkova.gridu.sparkappfraud;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;
import java.util.logging.Logger;
import java.lang.InterruptedException;
import java.util.Arrays;
import java.util.stream.Stream;

import com.datastax.spark.connector.writer.TTLOption;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.org.apache.xpath.internal.operations.Bool;
import com.sun.tools.javac.util.Pair;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.dstream.DStream;

import org.apache.spark.streaming.dstream.PairDStreamFunctions;
import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.streaming.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Array;
import scala.Tuple2;
import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

/*

Implement following rules to detect bots:

System should handle up to 200 000 events per minute and collect user click rate
and transitions for last 10 minutes.

Implement following requirements using Streaming API v1 (DStream) and Streaming API
v2(Structured streaming, Dataframes). And then compare the results of both computations.
 */


public class SparkDStream {

    public static class BotDataRow implements Serializable {
        private String ip;

        BotDataRow(){}
        BotDataRow(String ip) {
            this.ip = ip;
        }
        public String getIp() { return ip; }
        public void setIp(String ip) { this.ip = ip; }
    }

    public static class CtrDataRow implements Serializable {
        private String ip;
        private Double ctr;

        CtrDataRow(){}

        CtrDataRow(String ip, Double ctr){
            this.ip = ip;
            this.ctr = ctr;
        }

        public String getIp() { return ip; }
        public void setIp(String ip) { this.ip = ip; }

        public Double getCtr() { return ctr; }
        public void setCtr(Double ctr) { this.ctr = ctr; }
    }

    public static class FullDataRow implements Serializable {
        private String ip;
        private Double ctr;
        private Double clicks;
        private Double views;
        private Set<String> categoriesIds = new HashSet<String>();
        private Boolean bot = false;

        FullDataRow(){}

        FullDataRow(Boolean bot){
            this.bot = bot;
        }
        FullDataRow(String ip, Double clicks, Double views, String categoriesIds){
            this.ip = ip;
            this.clicks = clicks;
            this.views = views;
            this.categoriesIds.add(categoriesIds);
        }

        FullDataRow(String ip, Double clicks, Double views, Set<String> categoriesIds){
            this.ip = ip;
            this.clicks = clicks;
            this.views = views;
            this.categoriesIds = categoriesIds;
        }

        FullDataRow(String ip, Double clicks, Double views, Set<String> categoriesIds, Double ctr){
            this.ip = ip;
            this.clicks = clicks;
            this.views = views;
            this.categoriesIds = categoriesIds;
            this.ctr = ctr;
        }

        public String getIp() { return ip; }
        public void setIp(String ip) { this.ip = ip; }

        public Boolean getBot() { return bot; }
        public void setBot(Boolean bot) { this.bot = bot; }

        public Double getClicks() { return clicks; }
        public void setClicks(Double clicks) { this.clicks = clicks; }

        public Double getViews() { return clicks; }
        public void setViews(Double views) { this.views = views; }

        public Double getCtr() { return ctr; }
        public void setCtr(Double ctr) { this.ctr = ctr; }

        public Set<String> getCategoriesIds() { return categoriesIds; }
        public void setCategoriesIds(String categoriesIds) {
            this.categoriesIds.add(categoriesIds); }
        public Set<String> joinCategoriesIds(Set<String> categoriesIds) {
            this.categoriesIds.addAll(categoriesIds);
            return categoriesIds;
        }
    }

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

        final Collection<String> topics = Arrays.asList(SparkApp.prop.getProperty("kf.topics").split(","));

        JavaStreamingContext ssc = new JavaStreamingContext(app.conf,
                new Duration(1));

        Map<String, Object> kafkaParams = app.getDriverKafkaConf();
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams));

        ObjectMapper map = new ObjectMapper();

        Set<String> categoriesIds = new HashSet<String>();
        // Get the lines, split them into words, count the words and print
        JavaPairDStream<String, FullDataRow> lines = messages.mapToPair(s -> new Tuple2<>(s.key(),
                new FullDataRow(s.key(),
                s.topic().equals("clicks") ? 1.0 : 0,
                s.topic().equals("views") ? 1.0 : 0,
                        map.readTree(s.value()).get("category_id").asText())
        )).reduceByKeyAndWindow((a, b) -> new FullDataRow(a.ip, a.clicks+b.clicks,
                a.views+b.views, a.joinCategoriesIds(b.categoriesIds)),
                Durations.seconds(windowSec), Durations.seconds(slideSec)).mapValues(
                        s -> new FullDataRow(s.ip, s.clicks, s.views > 0 ? s.views : nullViews,
                                s.categoriesIds, s.clicks/s.views));

        JavaDStream<CtrDataRow> ctr = lines.map(s -> (new CtrDataRow(s._1, s._2.ctr)));

        Map<String, String> ctrColumnNameMappings = new HashMap<String, String>();
        ctrColumnNameMappings.put("ip", "ip");
        ctrColumnNameMappings.put("ctr", "ctr");

        javaFunctions(ctr).writerBuilder(keyspace, ctrTable, CassandraJavaUtil.mapToRow(CtrDataRow.class,
                ctrColumnNameMappings)).withConstantTTL(ttl).saveToCassandra();

        JavaDStream<FullDataRow> define_bots = lines.map(
                s -> new FullDataRow(s._2.clicks + s._2.views > maxEvents
                        || s._2.ctr > clicksToViewsMax  || s._2.categoriesIds.size() > 5));

        JavaDStream<BotDataRow> bots = define_bots.filter(s -> s.bot).map(s -> new BotDataRow(s.ip));


        Map<String, String> columnNameMappings = new HashMap<String, String>();
        columnNameMappings.put("ip", "ip");

        javaFunctions(bots).writerBuilder(keyspace, botTable, CassandraJavaUtil.mapToRow(BotDataRow.class,
                columnNameMappings)).withConstantTTL(ttl).saveToCassandra();

        ssc.start();
        ssc.awaitTermination();
    }



}
