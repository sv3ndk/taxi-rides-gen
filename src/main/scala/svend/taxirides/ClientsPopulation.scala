package svend.taxirides

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.lightbend.kafka.scala.streams._
import com.sksamuel.avro4s._
import org.apache.kafka.streams.Consumed
import svend.taxirides.Client.ClientSerializer
import svend.toolkit.{Generators, Population, PopulationMember}

import scala.util.Random


object ClientPopulation {

  def apply(size: Int, zonePopulation: Population[Zone])(implicit builder: StreamsBuilderS, consumed: Consumed[String, Client]) =
    new Population[Client] {
      override val allMemberids = Generators.sequencialGen("cl").take(size)

      override def randomMemberId: String = s"cl-${Random.nextInt(size)}"

      /**
        * create a client this this member id, a random name and a random initial zone (picked form the zone population)
        * */
      def randomClient(id: String) = Client(id, Generators.englishNameGen(), zonePopulation.randomMemberId)

      /**
      * provides one random member id of this population
      **/
      override val members = Population.build(
        allMemberids.map(randomClient),
        Config.topics.clientPopulation,
        classOf[ClientSerializer],
        builder
      )

    }

}


/**
  * Client are the agent that are part of a population. THey only contain an id
  * for now, we'll add more attributes later,...
  * */
case class Client(id: String, name: String, currentLocation: String) extends PopulationMember

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

