package svend.taxirides

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.lightbend.kafka.scala.streams.ImplicitConversions._
import com.lightbend.kafka.scala.streams.{KTableS, StreamsBuilderS}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.streams.kstream.{Materialized, Transformer}
import org.apache.kafka.streams.processor.{ProcessorContext, PunctuationType}
import org.apache.kafka.streams.state.Stores


/*
  * This is responsible for creating the Client's Population, as a KTable.
  * --config delete.retention.ms=1000
  *
  * First create a topic. We use a super short retention period

  kafka-topics                            \
  --create                              \
  --zookeeper localhost:2181            \
  --partitions 1                        \
  --replication-factor 1                \
  --config cleanup.policy=compact   \
  --topic taxirides-population-clients



  * */


object Clients {

  type ClientId = String
  type Client = String


  /**
    * populates data in Kafka for the client's population (in a compacted topic)
    * */
  def populateMembers(): Unit = {

    val props = Config.kafkaProducerProps
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val clientProducer = new KafkaProducer[ClientId, Client](props)

    Generators.sequencialGen("cl").take(20).foreach { client =>
      clientProducer.send( new ProducerRecord[ClientId, Client](Config.topics.clientPopulation, client, client) )
    }

    clientProducer.close()
  }

  def population(builder: StreamsBuilderS) = {
    builder.table[ClientId, Client](Config.topics.clientPopulation)
  }
}
