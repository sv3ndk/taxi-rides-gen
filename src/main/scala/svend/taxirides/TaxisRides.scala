package svend.taxirides

import com.lightbend.kafka.scala.streams.{KTableS, StreamsBuilderS}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.Printed
import svend.toolkit.{Related, Relationship, Stories}


/**
  * Main entry-point of the project
  * */
object TaxiRides extends App {

  val nClients = 50
  ClientsPopulation.populateMembers(nClients)

  val builder = new StreamsBuilderS
  val clientsPopulation = ClientsPopulation.population(builder)

  clientsPopulation.toStream.print(Printed.toSysOut[String, Client])

  val friendsRelationship = Relationship.generateBidirectionalRelations(builder,
    clientsPopulation, nClients/ 3, 2)

  friendsRelationship.toStream.print(Printed.toSysOut[String, Related])

  val taxiRidesLogs = TaxiRidesScenario.addTaxiRidesStory(builder, clientsPopulation)
  taxiRidesLogs.print(Printed.toSysOut[String, String])

  val app = new KafkaStreams(builder.build, Config.kafkaStreamsProps)

  // resets the state: for a data-generator, this is necessary since we have no input data: we want to
  // forget any past state between execution and start creating new one
  app.cleanUp()
  app.start()

}


/**
  * utility methods to create the various parts of the taxi rides scenario
  * */
object TaxiRidesScenario {

  /**
    * Builds the taxi ride story, in which Clients hail taxis in their current zone and get a ride
    * to one of their favourite zones.
    * */
  def addTaxiRidesStory(builder: StreamsBuilderS, clientsPopulation: KTableS[String, Client]) = {

    // at the moment, we just trigger the "taxi ride" story repeatedly for random actors, and nothing else happens after...
    val triggeredIdStream = Stories
      .buildTrigger(builder, "taxiRides", clientsPopulation)

    // TODO:
    // add a selectOne Transformer the RelationShip in the toolkit, similarly to the Trigger => has acces to the statestore
    // and just calls a selectOne from there

    triggeredIdStream

  }

}




