package svend.taxirides

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.lightbend.kafka.scala.streams.{Deserializer, ScalaSerde, Serializer}
import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream}


/**

  There must be a better way than copy/pasting those 2 Avro serializers every time...

  * */
object DamnYouSerdes {


  implicit object Tuple2StringsSerdes extends ScalaSerde[(String, String)] {

    override def serializer() = new Serializer[(String, String)] {
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

}
