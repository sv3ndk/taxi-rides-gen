# Taxi rides generator

This is a quick proof of concept of a data generator based on Kafka Streams. Most of the concepts are inspired from [Trumania](https://github.com/RealImpactAnalytics/trumania), in a much more basic but also more scalable fashion.


In order to run, first create the necessary Kafka topics:
```
  kafka-topics                            \
  --create                              \
  --zookeeper localhost:2181            \
  --partitions 1                        \
  --replication-factor 1                \
  --config cleanup.policy=compact   \
  --topic taxirides-population-clients-2

```

Then simply launch `run` from the sbt console.


