Spark SQL and Spark Streaming with Kafka
========================================


How ro Run
----------

Build fat jar: `sbt assembly`.

Run `docker-compose run --rm --service-ports java`.

In the `java` container terminal run:

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
docker exec -it $(docker-compose ps -q kafka) kafka-console-producer.sh --broker-list localhost:9092 --topic events
```

And add JSON events like:

```
{"action":"create","timestamp":"2017-10-05T23:01:17Z"}
{"action":"update","timestamp":"2017-10-05T23:01:19Z"}
{"action":"update","timestamp":"2017-10-05T23:02:51Z"}
```
