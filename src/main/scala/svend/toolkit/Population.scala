package svend.toolkit

import com.lightbend.kafka.scala.streams.{KTableS, Serializer, StreamsBuilderS}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.streams.Consumed
import svend.taxirides.Config


trait PopulationMember {
  val id: String
}


trait Population[M <: PopulationMember ]{

  /**
    * bounded Stream providing all member ids of this population
    * */
  val allMemberids: Stream[String]

  /**
    * provides one random member id of this population
    * */
  def randomMemberId: String

  val members: KTableS[String, M]

}


object Population {

  /**
    * initialize a Population buy pushing some generated members to a topic and reading
    * them back as a KTable
    *
    * */
  def build[M <: PopulationMember](membersGenerator: Stream[M],
                                   kafkaTopic: String,
                                   memberSerializerClass: Class[_ <: Serializer[M]],
                                   builder: StreamsBuilderS)
  (implicit consumed: Consumed[String, M]) = {


    val props = Config.kafkaProducerProps
    props.put("value.serializer", memberSerializerClass.getName)

    val clientProducer = new KafkaProducer[String, M](props)

    membersGenerator
      .foreach { member => clientProducer.send( new ProducerRecord(kafkaTopic, member.id, member) ) }

    clientProducer.close()

    builder.table[String, M](kafkaTopic)
  }

}

