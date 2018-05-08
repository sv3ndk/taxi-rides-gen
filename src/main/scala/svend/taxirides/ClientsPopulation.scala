package svend.taxirides

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.lightbend.kafka.scala.streams.ImplicitConversions._
import com.lightbend.kafka.scala.streams._
import com.sksamuel.avro4s._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


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
  --topic taxirides-population-clients-2

  * */

/**
  * Client are the agent that are part of a population
  * */
case class Client(id: String)

object Client {

  implicit object ClientSerdes extends ScalaSerde[Client] {
    override def deserializer() = new ClientDeserializer
    override def serializer() = new ClientSerializer
  }

  class ClientSerializer extends Serializer[Client] {
    override def serialize(data: Client): Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[Client](baos)
      output.write(data)
      output.close()
      baos.toByteArray
    }
  }

  class ClientDeserializer extends Deserializer[Client] {
    override def deserialize(data: Array[Byte]): Option[Client] = {
      val in = new ByteArrayInputStream(data)
      val input = AvroInputStream.binary[Client](in)
      Option(input.iterator.toSeq.head)
    }
  }

}


object ClientsPopulation {

  /**
    * populates data in Kafka for the client's population (in a compacted topic)
    * */
  def populateMembers(n: Int): Unit = {

    val props = Config.kafkaProducerProps
    props.put("value.serializer", "svend.taxirides.Client$ClientSerializer")

    val clientProducer = new KafkaProducer[String, Client](props)

    val idGenerator = Generators.sequencialGen("cl")

    idGenerator
      .take(n)
      .foreach { clientId =>
        val client = Client(clientId)
        clientProducer.send( new ProducerRecord(Config.topics.clientPopulation, clientId, client) )
      }

    clientProducer.close()
  }

  def population(builder: StreamsBuilderS): KTableS[String, Client] = {
    builder.table[String, Client](Config.topics.clientPopulation)
  }

}
