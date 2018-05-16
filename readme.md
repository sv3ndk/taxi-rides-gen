# Taxi rides generator

This is a quick proof of concept of a data generator based on Kafka Streams. Most of the concepts are inspired from [Trumania](https://github.com/RealImpactAnalytics/trumania), in a much more basic but also more scalable fashion.


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
[info] [KSTREAM-SOURCE-0000000047]: cl-32, (clientId: cl-32, clientName: Deidre Evanston, origin: z-21, destinationId: z-9, distance: 9.118)
[info] [KSTREAM-SOURCE-0000000047]: cl-39, (clientId: cl-39, clientName: Sam Smith, origin: z-23, destinationId: z-17, distance: 4.528)
[info] [KSTREAM-SOURCE-0000000047]: cl-41, (clientId: cl-41, clientName: Naeem Baker, origin: z-6, destinationId: z-8, distance: 7.241)
[info] [KSTREAM-SOURCE-0000000047]: cl-25, (clientId: cl-25, clientName: Natalie O'Toole, origin: z-14, destinationId: z-12, distance: 3.083)
[info] [KSTREAM-SOURCE-0000000047]: cl-40, (clientId: cl-40, clientName: Dave Dingleham, origin: z-17, destinationId: z-8, distance: 6.506)
[info] [KSTREAM-SOURCE-0000000047]: cl-12, (clientId: cl-12, clientName: Sam Havilland, origin: z-13, destinationId: z-14, distance: 7.676)
[info] [KSTREAM-SOURCE-0000000047]: cl-15, (clientId: cl-15, clientName: Naeem Schistton, origin: z-1, destinationId: z-16, distance: 5.112)
[info] [KSTREAM-SOURCE-0000000047]: cl-42, (clientId: cl-42, clientName: Mark Magnets, origin: z-17, destinationId: z-16, distance: 7.819)
[info] [KSTREAM-SOURCE-0000000047]: cl-44, (clientId: cl-44, clientName: David Magnets, origin: z-11, destinationId: z-2, distance: 5.395)
[info] [KSTREAM-SOURCE-0000000047]: cl-5, (clientId: cl-5, clientName: Sally Magnets-Alan, origin: z-20, destinationId: z-11, distance: 3.038)
[info] [KSTREAM-SOURCE-0000000047]: cl-10, (clientId: cl-10, clientName: Kate Schistham, origin: z-21, destinationId: z-19, distance: 3.957)
[info] [KSTREAM-SOURCE-0000000047]: cl-18, (clientId: cl-18, clientName: Terry Partridge, origin: z-16, destinationId: z-20, distance: 4.766)
[info] [KSTREAM-SOURCE-0000000047]: cl-37, (clientId: cl-37, clientName: Alan Corby, origin: z-8, destinationId: z-7, distance: 6.349)
[info] [KSTREAM-SOURCE-0000000047]: cl-48, (clientId: cl-48, clientName: Kevin Dingle, origin: z-11, destinationId: z-2, distance: 5.395)

```




Between execution, you have to reset the state of the stream, otherwise it picks up from the state of the of last execution, and adds new population, relationship, events... on top of that.

```sh
kafka-streams-application-reset \
    --bootstrap-servers localhost:9092  \
    --application-id taxi-rides-gen
```

