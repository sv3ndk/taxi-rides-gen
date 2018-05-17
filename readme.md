# Taxi rides generator

## Overview
This is a toyish proof of concept of a data generator based on Kafka Streams.

It consists of a taxi ride scenario (here [TaxiRides.scala](src/main/scala/svend/taxirides/TaxisRides.scala)) containing:

- a set of geographical zones
- a population of Taxis, having a current position, identified with a simple zone id
- a population of Clients, also having a current position
- a relationship from each client to their favourite set of zones
- a taxi ride story, in which client perform the following steps a random time interval:
   - select randomly a destination zone, among their favourite zones
   - select randomly a taxi present in their current zone
   - emit a log for the generated taxi ride
   - update their position and the taxi position.

There are race conditions a bit everywhere. For example nothing prevents 2 clients to pick the same taxi concurrently.

The implementation is based on joins between auto-updating materialized view (aka Ktables) with Kafka Streams.

Performance is not too good, I guess all those serializations come with a price...

Most of the concepts are inspired from [Trumania](https://github.com/RealImpactAnalytics/trumania), in a much more basic but also more scalable fashion.

## How to Run

In order to run, first create the necessary Kafka topics:

```sh

 # this is a changelog of full client's profile
  kafka-topics                            \
  --create                              \
  --zookeeper localhost:2181            \
  --partitions 1                        \
  --replication-factor 1                \
  --config cleanup.policy=compact   \
  --topic taxirides-population-clients

# changelog for geographical zones
  kafka-topics                            \
  --create                              \
  --zookeeper localhost:2181            \
  --partitions 1                        \
  --replication-factor 1                \
  --config cleanup.policy=compact   \
  --topic taxirides-population-zones

# changelog for geographical zones
  kafka-topics                            \
  --create                              \
  --zookeeper localhost:2181            \
  --partitions 1                        \
  --replication-factor 1                \
  --config cleanup.policy=compact   \
  --topic taxirides-population-taxis

# changelog for zone to zone distance matrix
  kafka-topics                            \
  --create                              \
  --zookeeper localhost:2181            \
  --partitions 1                        \
  --replication-factor 1                \
  --config cleanup.policy=compact   \
  --topic taxirides-relationship-zones2zonedistance

  # technical topic, used for the shuffle when looking up zone-to-zone disatnce
  kafka-topics                            \
  --create                              \
  --zookeeper localhost:2181            \
  --partitions 1                        \
  --replication-factor 1                \
  --topic taxirides-tech-zonedistance

  # technical topic, used for the shuffle when looking up zone-to-zone disatnce
  kafka-topics                            \
  --create                              \
  --zookeeper localhost:2181            \
  --partitions 1                        \
  --replication-factor 1                \
  --topic taxirides-tech-zone

  # technical topic, used by doing the last key-by client
  kafka-topics                            \
  --create                              \
  --zookeeper localhost:2181            \
  --partitions 1                        \
  --replication-factor 1                \
  --topic taxirides-tech-keybyclient

```

Then simply launch `run` from the sbt console. The console should start outputing random taxi rides similar to:

```sh
[info] [KSTREAM-SOURCE-0000000059]: cl-77, (clientId: cl-77, clientName: Robert Corby, withTaxi: t-31, from: z-8, destinationId: z-8, distance: 1.502)
[info] [KSTREAM-SOURCE-0000000059]: cl-81, (clientId: cl-81, clientName: Dave Toastson, withTaxi: t-21, from: z-0, destinationId: z-1, distance: 3.847)
[info] [KSTREAM-SOURCE-0000000059]: cl-82, (clientId: cl-82, clientName: Derek Bordeham, withTaxi: t-30, from: z-6, destinationId: z-9, distance: 4.556)
[info] [KSTREAM-SOURCE-0000000059]: cl-84, (clientId: cl-84, clientName: Deidre Magnets, withTaxi: t-25, from: z-1, destinationId: z-6, distance: 0.417)
[info] [KSTREAM-SOURCE-0000000059]: cl-45, (clientId: cl-45, clientName: Maggie Thorpeton, withTaxi: t-12, from: z-1, destinationId: z-5, distance: 9.059)
[info] [KSTREAM-SOURCE-0000000059]: cl-37, (clientId: cl-37, clientName: Sally Dingleham, withTaxi: t-7, from: z-7, destinationId: z-3, distance: 1.342)
[info] [KSTREAM-SOURCE-0000000059]: cl-40, (clientId: cl-40, clientName: Penny Evans, withTaxi: t-39, from: z-6, destinationId: z-5, distance: 1.911)
[info] [KSTREAM-SOURCE-0000000059]: cl-41, (clientId: cl-41, clientName: Natalie Smithford, withTaxi: t-23, from: z-4, destinationId: z-9, distance: 3.619)
[info] [KSTREAM-SOURCE-0000000059]: cl-87, (clientId: cl-87, clientName: Derek Twerp, withTaxi: t-4, from: z-5, destinationId: z-5, distance: 9.501)
[info] [KSTREAM-SOURCE-0000000059]: cl-92, (clientId: cl-92, clientName: Rahul Smith, withTaxi: t-6, from: z-8, destinationId: z-7, distance: 9.580)


```

Between executions, you have to reset the state of the stream, otherwise it picks up from the state of the of last execution, and adds new population, relationship, events... on top of that.
(note that reset does not work too well on compacted topics, which is what we use for containing the populations)

```sh
kafka-streams-application-reset \
    --bootstrap-servers localhost:9092  \
    --application-id taxi-rides-gen
```

