#### How to run full project

##### Prerequisites

    Docker && Docker Compose (with support 3.6 version)
    Maven
    
##### Fast run

    git clone https://github.com/jvolkova-ias/fraud
    
    cd spark/sparkappfraud && mvn package && cd ../../
    cd flume/jsoninterceptor && mvn package && cd ../../
    docker-compose up 

Those steps will up all parts of project and generator will start produce source data. 
Before start please setup how much data you want inside:

    ./fraudocker/botgen/botgen.dockerfile 
    
or you can start generating source data manually if you wish.


Casandra start with init from schemas, you don't need manual init. If you want, you can 
check correct init Cassandra with command (from cqlsh):
        
        DESCRIBE botdetect;
        

##### Source paths

Source data will be fetched from ${PWD}/source_data directory by default::
    
    ./fraudocker/source_data
    
You can change this by editing docker-compose file (volume section)
    
And source data file for Flume is:

    source.json

You can change this by editing docker-compose file (volume section)


#### System short-describe

All tested only in local mode

Services (after colon) == service name in docker-compose file

[partners : botgen] => [file disk : source_data/source.json] => [flume] => [kafka] => [spark: spark_ds & spark_df] => [cassandra]

**Flume**:
    - read data from source, set headers by custom JasonInterceptor: topic (views/clicks), 
    timestamp, key (ip); remove redundant data from body(value) of event - unix_time and type
    - send to Kafka Sinks per topic

**Kafka**:
    - work with 2 topics, each 1 replication, 3 partitions

**Spark**:
    - work with 3 threads in local mode
    - all settings in configs 
    - two variations Streams:
        1) jvolkova.gridu.sparkdstream.SparkDstream class, run with: 
    
            ./bin/spark-submit --verbose --class jvolkova.gridu.sparkdstream.SparkDStream --master local[3] --deploy-mode client  local:/jars/sparkconnectors.jar
        
        
   2) jvolkova.gridu.sparkdstream.SparkStructuredStream class, run with:
            
        
        ./bin/spark-submit --verbose --class jvolkova.gridu.sparkdstream.SparkStructuredStreamclass --master local[3] --deploy-mode client  local:/jars/sparkconnectors.jar
           
   
   - spark logic: 
   
   1) filter bots by big events count in window 
   "Enormous event rate, e.g. more than 1000 request in 10 minutes*." 
   reduceByKeyAndWindow
   Write bots with ttl to cassandra
   
   2) after filter Looking for many categories during the period, e.g. 
   more than 5 categories in 10 minutes.
   Write bots with ttl to cassandra
   
   3) High difference between click and view events, e.g. (clicks/views) more than 3-5. 
   Correctly process cases when there is no views. If no views set up normal if 1 click
 