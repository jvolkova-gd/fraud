package main.java.jvolkova.gridu.sparkappfraud;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.SparkConf;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import main.java.jvolkova.gridu.sparkappfraud.dataclasses.FullDataRow;
import main.java.jvolkova.gridu.sparkappfraud.dataclasses.BotDataRow;
import main.java.jvolkova.gridu.sparkappfraud.dataclasses.CtrDataRow;
import scala.Tuple2;

import java.util.HashMap;

public class SparkStructuredStream {

    public static void main(String[] args) throws InterruptedException {
        SparkApp app = new SparkApp();

        final Integer windowSec = Integer.parseInt(app.getAnalytic("window"));
        final Integer slideSec = Integer.parseInt(app.getAnalytic("slide"));
        final Double nullViews = Double.parseDouble(app.getAnalytic("null.views"));
        final Integer maxEvents = Integer.parseInt(app.getAnalytic("maxevents.perip"));
        final Integer maxCat = Integer.parseInt(app.getAnalytic("maxcat.perip"));
        final Double clicksToViewsMax = Double.parseDouble(app.getAnalytic("clicksToViewsMax"));
        final String keyspace = app.prop.getProperty("cassandra.keyspace.dataframe");
        final String botTable = app.prop.getProperty("cassandra.table.bots");
        final String ctrTable = app.prop.getProperty("cassandra.table.ctr");

        JavaStreamingContext ssc = new JavaStreamingContext(app.conf,
                new Duration(1));

        JavaInputDStream<ConsumerRecord<String, String>> messages = app.getStream(ssc, "df");

        ObjectMapper map = new ObjectMapper();

        JavaPairDStream<String, FullDataRow> lines = messages.mapToPair(s -> new Tuple2<>(s.key(),
                new FullDataRow(s.key(),
                        s.topic().equals("clicks") ? 1.0 : 0,
                        s.topic().equals("views") ? 1.0 : 0,
                        map.readTree(s.value()).get("category_id").asText())
        )).reduceByKeyAndWindow((a, b) -> new FullDataRow(a.getIp(), a.getClicks() + b.getClicks(),
                        a.getViews() + b.getViews(), a.joinCategoriesIds(b.getCategoriesIds())),
                Durations.seconds(windowSec), Durations.seconds(slideSec)).mapValues(
                s -> new FullDataRow(s.getIp(), s.getClicks(), s.getViews() > 0 ? s.getViews() : nullViews,
                        s.getCategoriesIds(), s.getClicks() / s.getViews()));

        JavaDStream<FullDataRow> lines_with_bots = lines.map(
                s -> new FullDataRow(s._2.getClicks() + s._2.getViews() > maxEvents
                        || s._2.getCtr() > clicksToViewsMax  || s._2.getCategoriesIds().size() > maxCat));

        JavaDStream<CtrDataRow> ctr = lines.map(s -> (new CtrDataRow(s._1, s._2.getCtr())));

        lines_with_bots.foreachRDD((rdd, time) -> {
            SparkSession spark = JavaSparkSessionSingleton.getInstance(
                    rdd.context().getConf());

            JavaRDD<CtrDataRow> ctrRowRDD = rdd.map(row -> {
                CtrDataRow record = new CtrDataRow();
                record.setIp(row.getIp());
                record.setCtr(row.getCtr());
                return record;
            });
            Dataset<Row> ctrDataFrame = spark.createDataFrame(ctrRowRDD, CtrDataRow.class);

            ctrDataFrame.write().format("org.apache.spark.sql.cassandra")
                    .options(new HashMap<String, String>() {
                        {
                            put("keyspace", keyspace);
                            put("table", ctrTable);
                        }
                    }).save();

            JavaRDD<BotDataRow> botsRowRDD = rdd.filter(
                    s -> s.getBot().equals(true)).map(row -> {
                BotDataRow record = new BotDataRow();
                record.setIp(row.getIp());
                return record;
            });

            Dataset<Row> botDataFrame = spark.createDataFrame(ctrRowRDD, CtrDataRow.class);

            botDataFrame.write().format("org.apache.spark.sql.cassandra")
                    .options(new HashMap<String, String>() {
                        {
                            put("keyspace", keyspace);
                            put("table", botTable);
                        }
                    }).save();

        });

        ssc.checkpoint("/spark-system/df");
        ssc.start();
        ssc.awaitTermination();
}
}

class JavaSparkSessionSingleton {
    private static transient SparkSession instance = null;
    public static SparkSession getInstance(SparkConf sparkConf) {
        if (instance == null) {
            instance = SparkSession
                    .builder()
                    .config(sparkConf)
                    .getOrCreate();
        }
        return instance;
    }
}
