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
    *
    * Populates random relationships among the members of that population.
    * For every relation from a to b, there will be a relation from b to a
    *
    * */
  def generateBidirectionalRelations[A](builder: StreamsBuilderS, population: KTableS[String, A],
                                        nGroups: Int, ngroupsPerMember: Int): KTableS[String, Related] = {

    val random = new Random

    population
      .toStream
      // assign each person to several random group
      .flatMap { case (memberId, _) => (1 to ngroupsPerMember).map( _ => random.nextInt(nGroups) -> Related(memberId) ) }

      // from each group, builds the set of all members in it
      .groupByKey.reduce( _ + _)

      // for each person in the group, mark all the other people as their friends
      .toStream
      .flatMap { case (groupId, friends) =>
        friends.ids.map { id => id -> (friends - id) }
      }
      .groupByKey.reduce(_ + _)

  }


}
