package svend.taxirides

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.lightbend.kafka.scala.streams._
import com.sksamuel.avro4s._
import svend.toolkit.{Generators, PopulationMember}

/**
  * Client are the agent that are part of a population. THey only contain an id
  * for now, we'll add more attributes later,...
  * */
case class Client(id: String, name: String, currentLocation: Option[String]) extends PopulationMember

object Client {

  /**
    * Generator of random Client population members
    * */
  def clientGen(size: Int) = Generators.sequencialGen("cl").map(Client(_, Generators.englishNameGen(), None)).take(size)

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

