package svend.toolkit

import com.lightbend.kafka.scala.streams.{Serializer, StreamsBuilderS}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.streams.Consumed
import svend.taxirides.Config


trait PopulationMember {
  val id: String
}

object Population {

  /**
    * generates memeber of this population and push them to a (compacted) Kafka topic
    * */
  def populateMembers[M <: PopulationMember ](builder:  StreamsBuilderS, memberGenerator: Stream[M],
                                              memberSerializerClass: Class[_ <: Serializer[M]], kafkaTopic: String)
                                             (implicit consumed: Consumed[String, M])= {

    val props = Config.kafkaProducerProps
    props.put("value.serializer", memberSerializerClass.getName)

    val clientProducer = new KafkaProducer[String, M](props)

    memberGenerator
      .foreach { member => clientProducer.send( new ProducerRecord(kafkaTopic, member.id, member) ) }

    clientProducer.close()

    builder.table[String, M](kafkaTopic)

  }
}
