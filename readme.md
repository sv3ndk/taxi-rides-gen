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

```

Then simply launch `run` from the sbt console. The console should start outputing random taxi rides similar to:

```sh
[info] [KSTREAM-MAPVALUES-0000000037]: cl-49, (clientId: cl-49, clientName: Dave Magnetsham, origin: z-3, destinationId: z-16)
[info] [KSTREAM-MAPVALUES-0000000037]: cl-0, (clientId: cl-0, clientName: Mark O'Tooleford, origin: z-3, destinationId: z-18)
[info] [KSTREAM-MAPVALUES-0000000037]: cl-30, (clientId: cl-30, clientName: Natalie Doonford, origin: z-20, destinationId: z-15)
[info] [KSTREAM-MAPVALUES-0000000037]: cl-22, (clientId: cl-22, clientName: Maggie Rafferty, origin: z-8, destinationId: z-7)
[info] [KSTREAM-MAPVALUES-0000000037]: cl-28, (clientId: cl-28, clientName: Kelly Raffertyham, origin: z-6, destinationId: z-12)
[info] [KSTREAM-MAPVALUES-0000000037]: cl-22, (clientId: cl-22, clientName: Maggie Rafferty, origin: z-7, destinationId: z-20)
[info] [KSTREAM-MAPVALUES-0000000037]: cl-39, (clientId: cl-39, clientName: Gavin Jones, origin: z-15, destinationId: z-8)
[info] [KSTREAM-MAPVALUES-0000000037]: cl-10, (clientId: cl-10, clientName: Gladys Yarisham, origin: z-0, destinationId: z-1)
[info] [KSTREAM-MAPVALUES-0000000037]: cl-20, (clientId: cl-20, clientName: Dan Gravelson, origin: z-12, destinationId: z-10)
[info] [KSTREAM-MAPVALUES-0000000037]: cl-42, (clientId: cl-42, clientName: Maggie Greenston, origin: z-1, destinationId: z-20)
[info] [KSTREAM-MAPVALUES-0000000037]: cl-8, (clientId: cl-8, clientName: Kathryn Gravis, origin: z-21, destinationId: z-13)
[info] [KSTREAM-MAPVALUES-0000000037]: cl-13, (clientId: cl-13, clientName: Barry Magnets, origin: z-13, destinationId: z-10)


```




Between execution, you have to reset the state of the stream, otherwise it picks up from the state of the of last execution, and adds new population, relationship, events... on top of that.

```sh
kafka-streams-application-reset \
    --bootstrap-servers localhost:9092  \
    --application-id taxi-rides-gen
```

