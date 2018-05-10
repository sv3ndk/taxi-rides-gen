package svend.taxirides

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.lightbend.kafka.scala.streams._
import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream}
import svend.toolkit.{Generators, PopulationMember}

case class Zone(id: String) extends PopulationMember

object Zone {

  val zoneGen = Generators.sequencialGen("z").map(Zone(_))

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


