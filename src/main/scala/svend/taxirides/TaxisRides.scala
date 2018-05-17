package svend.taxirides

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.lightbend.kafka.scala.streams.ImplicitConversions._
import com.lightbend.kafka.scala.streams._
import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.Printed
import svend.toolkit.{Population, Related, Relationship, Stories}


/**
  * Main entry-point of the project
  **/
object TaxiRides extends App {

  val nClients = 100
  val nTaxis = 40
  val nZones = 10

  implicit val builder = new StreamsBuilderS

  // zone population, representing geographical zones where clients are taxis are located
  val zonePopulation = ZonePopulation(nZones)

  // taxis clients, will trigger the taxi rides scenario
  val clientsPopulation = ClientPopulation(nClients, zonePopulation)

  // taxis themselves, will be part of requested rides, and update their after each ride
  val taxiPopulation = TaxiPopulation(nTaxis, zonePopulation)
  val taxiPerZone = TaxiPopulation.taxiPerZoneRelationship(taxiPopulation)

  // client's favourite locations: when a taxi ride is generated, each client will go to one of their
  // favourite locations
  val favouriteLocations = Relationship.generateDirectionalRelations(
    clientsPopulation, zonePopulation, nZones / 2, 2)

  val zone2ZoneDistances = ZonePopulation.zoneToZoneDistanceRelationship(zonePopulation)

  //  val friendsRelationship = Relationship.generateBidirectionalRelations(builder,
  //    clientsPopulation, nClients/ 3, 2)

  val taxiRidesLogs = TaxiRidesScenario.addTaxiRidesStory(clientsPopulation, taxiPopulation, favouriteLocations, zone2ZoneDistances, taxiPerZone)
  taxiRidesLogs.print(Printed.toSysOut[String, TaxiRide])

  val app = new KafkaStreams(builder.build, Config.kafkaStreamsProps)

  // resets the state: for a data-generator, this is necessary since we have no input data: we want to
  // forget any past state between executions and start creating new one
  app.cleanUp()
  app.start()

}

case class TaxiRide(clientId: String, clientName: String, fromZone: String, toZone: String, distance: Double, taxiId: String) {
  override def toString: String = {
    f"(clientId: $clientId, clientName: $clientName, withTaxi: $taxiId, from: $fromZone, destinationId: $toZone, distance: $distance%.3f)"
  }
}

object TaxiRide {

  implicit object Serdes extends ScalaSerde[TaxiRide] {

    override def serializer() = new Serializer[TaxiRide] {
      override def serialize(data: TaxiRide): Array[Byte] = {
        val baos = new ByteArrayOutputStream()
        val output = AvroOutputStream.binary[TaxiRide](baos)
        output.write(data)
        output.close()
        baos.toByteArray
      }
    }

    override def deserializer() = new Deserializer[TaxiRide] {
      override def deserialize(data: Array[Byte]): Option[TaxiRide] = {
        val in = new ByteArrayInputStream(data)
        val input = AvroInputStream.binary[TaxiRide](in)
        Option(input.iterator.toSeq.head)
      }
    }

  }

}

/**
  * utility methods to create the various parts of the taxi rides scenario
  **/
object TaxiRidesScenario {

  /**
    * Builds the taxi ride story, in which Clients hail taxis in their current zone and get a ride
    * to one of their favourite zones.
    **/
  def addTaxiRidesStory(clientsPopulation: Population[Client],
                        taxisPopulation: Population[Taxi],
                        favouriteLocations: KTableS[String, Related],
                        zone2ZoneDistances: KTableS[(String, String), Double],
                        taxiPerZoneRelationships: KTableS[String, Related])
                       (implicit builder: StreamsBuilderS) = {

    import DamnYouSerdes._

    // main story logic: generates taxi rides from various populations and relationships
    val taxiRideLogs = Stories

      // trigger this story for some actors, repeatedly
      .buildTrigger(builder, "taxiRides", clientsPopulation)

      // select a random destination location among that client's favourite locations
      .join(favouriteLocations, (clientId: String, locations: Related) => (clientId, locations.selectOne))

      // looks up client's attributes
      .join(clientsPopulation.members, (clDest: (String, String), client: Client) =>
      (clDest._1, client.name, client.currentLocation, clDest._2))

      // looks up the distance between those two zones
      .selectKey { case (key, (_, _, fromZone, toZone)) => (fromZone, toZone) }
      .through(Config.topics.storyShuffleDistance)
      .join(zone2ZoneDistances,
        (ride: (String, String, String, String), distance: Double) =>
          (ride._1, ride._2, ride._3, ride._4, distance)
      )

      // select a random taxi that is available in that zone
      .selectKey { case (key, (_, _, fromZone, _, _) ) => fromZone}
      .through(Config.topics.storyShuffleZone)
      .join(taxiPerZoneRelationships,
        (ride: (String, String, String, String, Double), taxis: Related) =>
          (ride._1, ride._2, ride._3, ride._4, ride._5, taxis.selectOne)
      )

      // wraps it all up into a TaxiRide instance
      .mapValues {
      case (clientId: String, clientName: String, fromZone: String, toZone: String, distance: Double, taxiId: String) =>
        TaxiRide(clientId, clientName, fromZone, toZone, distance, taxiId)
    }
      .selectKey((k, ride) => ride.clientId)
      .through(Config.topics.storyShuffleKeyByClient)

    // updates the client's changelog by setting the new current location to their latest taxi ride destination
    taxiRideLogs
      .join(clientsPopulation.members, (ride: TaxiRide, client: Client) => client.copy(currentLocation = ride.toZone))
      .to(Config.topics.clientPopulation)

    // update the taxis' changelog by setting their new current location to their latest taxi ride destination
    taxiRideLogs
        .join(taxisPopulation.members, (ride: TaxiRide, taxi: Taxi) => taxi.copy(currentZone = ride.toZone))
        .to(Config.topics.taxiPopulation)

    taxiRideLogs

  }

}
