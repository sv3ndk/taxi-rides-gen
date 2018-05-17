package svend.taxirides

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.lightbend.kafka.scala.streams.ImplicitConversions._

import com.lightbend.kafka.scala.streams.{Deserializer, ScalaSerde, Serializer, StreamsBuilderS}
import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream}
import org.apache.kafka.streams.Consumed
import svend.taxirides.Taxi.TaxiSerializer
import svend.toolkit.{Generators, Population, PopulationMember, Related}

import scala.util.Random


object TaxiPopulation {

  def apply(size: Int, zonePopulation: Population[Zone])(implicit builder: StreamsBuilderS, consumed: Consumed[String, Taxi]) =
    new Population[Taxi] {
      override val allMemberids = Generators.sequencialGen("t").take(size)

      override def randomMemberId: String = s"cl-${Random.nextInt(size)}"

      val r = new Random()

      /**
        * create a client this this member id, a random name and a random initial zone (picked form the zone population)
        **/
      def randomTaxi(id: String) = Taxi(id, zonePopulation.randomMemberId, Random.nextDouble() * 10)

      /**
        * provides one random member id of this population
        **/
      override val members = Population.build(
        allMemberids.map(randomTaxi),
        Config.topics.taxiPopulation,
        classOf[TaxiSerializer],
        builder
      )

    }

  def taxiPerZoneRelationship(taxiPopulation: Population[Taxi]) = {

    def safeRemove(r1: Related, r2: Related) =
      (Option(r1), Option(r2)) match {
        case (None, _) => Related.empty
        case (Some(existing), None) => existing
        case (Some(existing), Some(removed)) => {
          val smaller = existing - removed

          //if no taxis are present in that zone, we just remove this aggregation for now
          if (smaller.ids.isEmpty)
            null
          else
            smaller
        }
      }

    taxiPopulation.members
      .groupBy { case (id, taxi) => (taxi.currentZone, Related(taxi.id)) }
      .reduce(_ + _, safeRemove)
  }
}

case class Taxi(id: String, currentZone: String, rate: Double) extends PopulationMember

object Taxi {

  implicit object ClientSerdes extends ScalaSerde[Taxi] {
    override def deserializer() = new TaxiDeserializer
    override def serializer() = new TaxiSerializer
  }

  class TaxiSerializer extends Serializer[Taxi] {
    override def serialize(data: Taxi): Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[Taxi](baos)
      output.write(data)
      output.close()
      baos.toByteArray
    }
  }

  class TaxiDeserializer extends Deserializer[Taxi] {
    override def deserialize(data: Array[Byte]): Option[Taxi] = {
      val in = new ByteArrayInputStream(data)
      val input = AvroInputStream.binary[Taxi](in)
      Option(input.iterator.toSeq.head)
    }
  }

}

