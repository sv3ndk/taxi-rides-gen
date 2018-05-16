package svend.taxirides

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.lightbend.kafka.scala.streams.ImplicitConversions._
import com.lightbend.kafka.scala.streams._
import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.DoubleSerializer
import svend.taxirides.Client.ClientSerializer
import svend.taxirides.Zone.ZoneSerializer
import svend.toolkit.{Generators, Population, PopulationMember}

import scala.util.Random

case class Zone(id: String) extends PopulationMember

object Zone {


  implicit object ClientSerdes extends ScalaSerde[Zone] {
    override def deserializer() = new ZoneDeserializer
    override def serializer() = new ZoneSerializer
  }

  class ZoneSerializer extends Serializer[Zone] {
    override def serialize(data: Zone): Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[Zone](baos)
      output.write(data)
      output.close()
      baos.toByteArray
    }
  }

  class ZoneDeserializer extends Deserializer[Zone] {
    override def deserialize(data: Array[Byte]): Option[Zone] = {
      val in = new ByteArrayInputStream(data)
      val input = AvroInputStream.binary[Zone](in)
      Option(input.iterator.toSeq.head)
    }
  }

}


/**
  * Population of geo-graphical zones. Each taxi ride will occur from a zone to another
  * */
object ZonePopulation {

  def apply(nZones: Int)(implicit builder: StreamsBuilderS) = new Population[Zone] {

    override val allMemberids = Generators.sequencialGen("z").take(nZones)

    override def randomMemberId: String = s"z-${Random.nextInt(nZones)}"

    override val members = Population.build(
      allMemberids.map(Zone(_)),
      Config.topics.zonePopulation,
      classOf[ZoneSerializer],
      builder
    )
  }


  /**
    * Builds a random KTable with an entry for each pair of zone and a random double as distance between those two zones
    * */
  def zoneToZoneDistanceRelationship(zonePopulation: Population[Zone])(implicit builder: StreamsBuilderS) = {

    import DamnYouSerdes._

    val pairsGen = for {
      z1 <- zonePopulation.allMemberids
      z2 <- zonePopulation.allMemberids
    } yield (z1, z2)


    val props = Config.kafkaProducerProps

    props.put("key.serializer", classOf[Tuple2StringsSerdes.Tuple2StringsSerializer].getName)
    props.put("value.serializer", classOf[DoubleSerializer].getName)

    val clientProducer = new KafkaProducer[(String, String), Double](props)
    val kafkaTopic = Config.topics.zone2ZoneDistanceRelations
    val maxDistance = 10
    val random = new Random

    pairsGen
      .foreach { twoZoneIds => clientProducer.send( new ProducerRecord(kafkaTopic, twoZoneIds, random.nextDouble() * maxDistance) ) }

    clientProducer.close()

    builder.table[(String, String), Double](kafkaTopic)

  }


}

