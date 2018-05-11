package svend.taxirides

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.lightbend.kafka.scala.streams.ImplicitConversions._
import com.lightbend.kafka.scala.streams.{KTableS, StreamsBuilderS}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.Printed
import svend.taxirides.Client.ClientSerializer
import svend.taxirides.Zone.ZoneSerializer
import svend.toolkit.{Population, Related, Relationship, Stories}


/**
  * Main entry-point of the project
  **/
object TaxiRides extends App {

  val nClients = 50
  val nZones = 24

  val builder = new StreamsBuilderS

  // taxis clients, will trigger the taxi rides scenario
  val clientsPopulation = Population.populateMembers[Client](builder, nClients, Client.clientGen,
    classOf[ClientSerializer], Config.topics.clientPopulation)
  //  clientsPopulation.toStream.print(Printed.toSysOut[String, Client])

  // zone population, representing geographical zones where clients are taxis are located
  val zonePopulation = Population.populateMembers[Zone](builder, nZones, Zone.zoneGen,
    classOf[ZoneSerializer], Config.topics.zonePopulation)
  //  zonePopulation.toStream.print(Printed.toSysOut[String, Zone])

  // client's favourite locations: when a taxi ride is generated, each client will go to one of their
  // favourite locations
  val favouriteLocations = Relationship.generateDirectionalRelations(builder,
    clientsPopulation, zonePopulation, nZones / 2, 2)

  //  val friendsRelationship = Relationship.generateBidirectionalRelations(builder,
  //    clientsPopulation, nClients/ 3, 2)

  //  favouriteLocations.toStream.print(Printed.toSysOut[String, Related])

  //  friendsRelationship.toStream.print(Printed.toSysOut[String, Related])

  val taxiRidesLogs = TaxiRidesScenario.addTaxiRidesStory(builder, clientsPopulation, favouriteLocations)
  taxiRidesLogs.print(Printed.toSysOut[String, TaxiRide])

  val app = new KafkaStreams(builder.build, Config.kafkaStreamsProps)

  // resets the state: for a data-generator, this is necessary since we have no input data: we want to
  // forget any past state between execution and start creating new one
  app.cleanUp()
  app.start()

}

case class TaxiRide(clientId: String, destinationId: String) {
  override def toString: String = s"(clientId: $clientId, destinationId: $destinationId)"
}

/**
  * utility methods to create the various parts of the taxi rides scenario
  **/
object TaxiRidesScenario {

  /**
    * Builds the taxi ride story, in which Clients hail taxis in their current zone and get a ride
    * to one of their favourite zones.
    **/
  def addTaxiRidesStory(builder: StreamsBuilderS,
                        clientsPopulation: KTableS[String, Client],
                        favouriteLocations: KTableS[String, Related]) = {
    Stories

      // trigger this story for some actors, repeatedly
      .buildTrigger(builder, "taxiRides", clientsPopulation)

      // select a random location amount that client's favourite locations
      .join(favouriteLocations, (clientId: String, locations: Related) => (clientId, locations.selectOne))

      .mapValues { case (clientId: String, destinationId: String) => TaxiRide(clientId, destinationId) }


  }

}
