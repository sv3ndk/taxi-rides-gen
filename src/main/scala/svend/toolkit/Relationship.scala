package svend.toolkit

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.lightbend.kafka.scala.streams.ImplicitConversions._
import com.lightbend.kafka.scala.streams.{Deserializer, KTableS, ScalaSerde, Serializer}
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
  def generateRelations[A](population: KTableS[String, A], nClients: Int): KTableS[String, Related] = {

    // let's say we aim for about 5 friends per people on averate (minus collisions)
    val nGroups = nClients / 5
    val random = new Random

    population
        // assign each person to a random group
        .mapValues( kv => random.nextInt(nGroups))

        // from each group, builds the set of all related member in it
        .groupBy{ case (id, groupid) => (groupid, Related(id)) }
        .reduce(_ + _, _ nop _) // the nop is an abuse here, though I never change the "group" of people anyhow..

        // for each person in the group, mark all the other people as their friends
        .toStream
        .flatMap{ case (group, friends) =>
          friends.ids.map { id => id -> (friends - id) }
        }
        .groupByKey
        .reduce(_ + _)

  }

}
