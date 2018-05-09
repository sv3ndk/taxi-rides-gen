package svend.toolkit

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.lightbend.kafka.scala.streams.ImplicitConversions._
import com.lightbend.kafka.scala.streams._
import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream}

import scala.util.Random

/**
  * Set of population members that are related to a population member.
  *
  * In a KTable, the key would be the id of a population memeber, and the Related would be the set
  * of ids that are related to it.
  *
  * */
case class Related(ids: Set[String]) {

  val random = new Random
  private val idsList = ids.toList

  /** merges two set of relations, with de-duplication */
  def +(other: Related) = new Related(this.ids ++ other.ids)

  def -(removedOne: String) = new Related(this.ids - removedOne)

  def nop(other: Related) = this

  /** returns one random friends among the */
  def selectOne: String = idsList(random.nextInt(idsList.length))
}

object Related {
  def apply(first: String) = new Related(Set(first))

  implicit object FriendsSerdes extends ScalaSerde[Related] {
    override def deserializer() = new FriendsDeserializer
    override def serializer() = new FriendsSerializer
  }

  class FriendsSerializer extends Serializer[Related] {
    override def serialize(data: Related): Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[Related](baos)
      output.write(data)
      output.close()
      baos.toByteArray
    }
  }

  class FriendsDeserializer extends Deserializer[Related] {
    override def deserialize(data: Array[Byte]): Option[Related] = {
      val in = new ByteArrayInputStream(data)
      val input = AvroInputStream.binary[Related](in)
      Option(input.iterator.toSeq.head)
    }
  }


}


object Relationship {

  /**
    * Populates random relationships among the members of that population
    * */
  def generateRelations[A]( builder: StreamsBuilderS, population: KTableS[String, A], nClients: Int): KTableS[String, Related] = {

    // let's say we aim for about 2*4 friends per people on average (minus collisions)
    val nGroups = nClients / 3
    val iterations = 3
    val random = new Random

    val friendsTopic = "taxirides-internal-friendsTopic-5"

    (1 to iterations).foreach { _ =>
      population
        // assign each person to a random group
        .mapValues(kv => random.nextInt(nGroups))

        // from each group, builds the set of all related member in it
        .groupBy { case (id, groupid) => (groupid, Related(id)) }
        .reduce(
          _ + _,

          // I think the remove is called after each iteration, since it considers the element got assigned to another group...
          (v1: Related, v2: Related) => v2
        )

        // for each person in the group, mark all the other people as their friends
        .toStream
        .flatMap { case (group, friends) =>
          friends.ids.map { id => id -> (friends - id) }
        }
        .to(friendsTopic)
    }

      builder
        .stream[String, Related](friendsTopic)
        .groupByKey
        .reduce(_ + _)

  }

}
