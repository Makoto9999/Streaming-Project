Spark SQL and Spark Streaming with Kafka
========================================
This application is based on a post written by Anatoliy Plastinin in 2017. For more details see [this post](http://blog.antlypls.com/blog/2017/10/15/using-spark-sql-and-spark-streaming-together/).

In this project, my application will receive the messages from Kafka and detect potential DDOS attacks.

How ro Run
----------

In the project root, run 'sbt assembly` to build fat jar.
Now you can find 'kafka-spark.jar' under 'build/'.

Run `docker-compose run --rm --service-ports java` to set up Java, Kafka and ZooKeeper containers. Keep this Java container open.

Open a new terminal and run 'docker ps' to check the container list.
move the `kafka-spark.jar` to the Java container '/build'.
move the `apache-access-log.txt` file to Kafka container.

Run 
```wget https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz```
to download it to '/build' in Java container and unpack it to distribute Spark.

In the java container terminal run:

```
KAFKA_BROKERS=kafka:9092 \
KAFKA_GROUP_ID=spark-streaming-demo \
KAFKA_TOPIC=events \
spark-2.2.0-bin-hadoop2.7/bin/spark-submit \
  --master local[*] \
  --class com.antlypls.blog.KafkaSparkDemo kafka-spark-demo.jar
```

In a separate terminal run

```
docker exec -it $(docker-compose ps -q kafka) bash
```
to get into Kafka container.

Run 'tail -n20000 apache-access-log.txt | kafka-console-producer.sh --broker-list localhost:9092 --topic events' to push the latest 2000 records to Kafka.

In the java container we will see the result. And the detected attacks will be saved to '/result' in java container.
