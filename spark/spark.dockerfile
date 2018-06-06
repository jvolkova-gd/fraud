FROM gettyimages/spark:2.3.0-hadoop-2.8 as spark_df

RUN apt-get update && apt-get install wget -y
RUN cd /usr/spark-2.3.0/jars && \
    wget http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.11/2.3.0/spark-streaming-kafka-0-10_2.11-2.3.0.jar && \
    wget http://central.maven.org/maven2/org/apache/kafka/kafka_2.12/1.1.0/kafka_2.12-1.1.0.jar && \
    wget http://central.maven.org/maven2/org/apache/kafka/kafka-clients/1.1.0/kafka-clients-1.1.0.jar
COPY ./spark.properties /usr/spark-2.3.0/conf/spark.properties
COPY ./spark-defaults.conf /usr/spark-2.3.0/conf/spark-defaults.conf
RUN cd /usr/spark-2.3.0/conf/ && rm spark-defaults.conf.template && mkdir -p /logs/spark
ENTRYPOINT ./bin/spark-submit --verbose --class jvolkova.gridu.sparkappfraud.SparkStructuredStream --master local[3] --deploy-mode client  local:/jars/sparkappfraud.jar


FROM gettyimages/spark:2.3.0-hadoop-2.8 as spark_ds

RUN apt-get update && apt-get install wget -y
RUN cd /usr/spark-2.3.0/jars && \
    wget http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.11/2.3.0/spark-streaming-kafka-0-10_2.11-2.3.0.jar && \
    wget http://central.maven.org/maven2/org/apache/kafka/kafka_2.12/1.1.0/kafka_2.12-1.1.0.jar && \
    wget http://central.maven.org/maven2/org/apache/kafka/kafka-clients/1.1.0/kafka-clients-1.1.0.jar && \
    wget http://central.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.11/2.3.0/spark-cassandra-connector_2.11-2.3.0.jar && \
    wget http://central.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-java_2.11/1.6.0-M1/spark-cassandra-connector-java_2.11-1.6.0-M1.jar && \
    wget http://central.maven.org/maven2/com/datastax/cassandra/cassandra-driver-core/3.5.0/cassandra-driver-core-3.5.0.jar
COPY ./spark.properties /usr/spark-2.3.0/conf/spark.properties
COPY ./spark-defaults.conf /usr/spark-2.3.0/conf/spark-defaults.conf
RUN cd /usr/spark-2.3.0/conf/ && rm spark-defaults.conf.template && mkdir -p /logs/spark
ENTRYPOINT ./bin/spark-submit --verbose --class jvolkova.gridu.sparkappfraud.SparkDStream --master local[3] --deploy-mode client local:/jars/sparkappfraud.jar

