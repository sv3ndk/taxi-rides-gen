package svend.taxirides

import com.lightbend.kafka.scala.streams.{KTableS, StreamsBuilderS}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.Printed


object TaxiRides extends App {

  Clients.populateMembers()

  val builder = new StreamsBuilderS()
  val clientsPopulation = Clients.population(builder)

  val taxiRidesLogs = TaxiRidesScenario.addTaxiRidesStory(builder, clientsPopulation)

  taxiRidesLogs.print(Printed.toSysOut[String, String])

  val streams = new KafkaStreams(builder.build, Config.kafkaStreamsProps)
  streams.start()

}


object TaxiRidesScenario {


  /**
    * Builds the taxi ride story, in which Clients hail taxis in their current zone and get a ride
    * to one of their favourite zones.
    * */
  def addTaxiRidesStory(builder: StreamsBuilderS, clientsPopulation: KTableS[Clients.ClientId, Clients.Client]) = {


    val (storyTimerStoreName, storyTimerStore, storyTriggerSupplier) =
      Stories.buildTrigger[Clients.ClientId, Clients.Client]("taxiRides")

    builder.addStateStore(storyTimerStore)

    clientsPopulation
      .toStream
      .transform(storyTriggerSupplier, storyTimerStoreName)

  }


}
