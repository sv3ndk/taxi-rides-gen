package svend.taxirides

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.lightbend.kafka.scala.streams.{Deserializer, ScalaSerde, Serializer}
import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream}


/**
  * *
  * There must be a better way than copy/pasting those 2 Avro serializers every time...
  *
  **/
object DamnYouSerdes {


  implicit object Tuple2StringsSerdes extends ScalaSerde[(String, String)] {

    override def serializer() = new Tuple2StringsSerializer()

    class Tuple2StringsSerializer extends Serializer[(String, String)] {
      override def serialize(data: (String, String)): Array[Byte] = {
        val baos = new ByteArrayOutputStream()
        val output = AvroOutputStream.binary[(String, String)](baos)
        output.write(data)
        output.close()
        baos.toByteArray
      }
    }

    override def deserializer() = new Deserializer[(String, String)] {
      override def deserialize(data: Array[Byte]): Option[(String, String)] = {
        val in = new ByteArrayInputStream(data)
        val input = AvroInputStream.binary[(String, String)](in)
        Option(input.iterator.toSeq.head)
      }
    }
  }


  implicit object Tuple4StringsSerdes extends ScalaSerde[(String, String, String, String)] {

    override def serializer() = new Serializer[(String, String, String, String)] {
      override def serialize(data: (String, String, String, String)): Array[Byte] = {
        val baos = new ByteArrayOutputStream()
        val output = AvroOutputStream.binary[(String, String, String, String)](baos)
        output.write(data)
        output.close()
        baos.toByteArray
      }
    }

    override def deserializer() = new Deserializer[(String, String, String, String)] {
      override def deserialize(data: Array[Byte]): Option[(String, String, String, String)] = {
        val in = new ByteArrayInputStream(data)
        val input = AvroInputStream.binary[(String, String, String, String)](in)
        Option(input.iterator.toSeq.head)
      }
    }
  }

  implicit object Tuple4StringsAndDoubleSerdes extends ScalaSerde[(String, String, String, String, Double)] {

    override def serializer() = new Serializer[(String, String, String, String, Double)] {
      override def serialize(data: (String, String, String, String, Double)): Array[Byte] = {
        val baos = new ByteArrayOutputStream()
        val output = AvroOutputStream.binary[(String, String, String, String, Double)](baos)
        output.write(data)
        output.close()
        baos.toByteArray
      }
    }

    override def deserializer() = new Deserializer[(String, String, String, String, Double)] {
      override def deserialize(data: Array[Byte]): Option[(String, String, String, String, Double)] = {
        val in = new ByteArrayInputStream(data)
        val input = AvroInputStream.binary[(String, String, String, String, Double)](in)
        Option(input.iterator.toSeq.head)
      }
    }
  }

}
