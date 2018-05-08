package svend.taxirides

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.lightbend.kafka.scala.streams.{KTableS, StreamsBuilderS}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{ProcessorContext, PunctuationType}
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder
import org.apache.kafka.streams.state.{KeyValueStore, Stores}

import scala.collection.JavaConverters._

object Stories {


  /**
    * Builds a story trigger for a population of type A.
    *
    * This returns a KStreamsS of Agent ids, when they are triggered.
    * */
  def buildTrigger[A](builder: StreamsBuilderS, storyName: String, population: KTableS[String, A]) = {

    val timerStoreName = s"${storyName}Timers"
    val timerStoreSupplier = Stores.inMemoryKeyValueStore(timerStoreName)

    val timerStoreBuilder = new KeyValueStoreBuilder[String, Long](
      timerStoreSupplier,
      stringSerde,
      longSerde,
      Time.SYSTEM
    )

    builder.addStateStore(timerStoreBuilder)

    population
      .toStream
      .transform(() => new ClientsTrigger[A](timerStoreName), timerStoreName)

  }

  /**
    * Trigger of a Story: responsible for maintaining a state with a counter for each population member
    * + decrementing it regularly and triggering story execution when needed
    *
    * A: type of the population agent itself (typically some case class)
    * */
  class ClientsTrigger[A](timerStoreName: String) extends Transformer[String, A, (String, String)] {

    var timers: KeyValueStore[String, Long] = _
    var context: ProcessorContext = _
    var timerGen = Generators.randPosInt(25)

    override def init(processorContext: ProcessorContext): Unit = {
      processorContext.schedule(100, PunctuationType.WALL_CLOCK_TIME, (timestamp: Long) => timeStep())
      timers = processorContext.getStateStore(timerStoreName).asInstanceOf[KeyValueStore[String, Long]]
      context = processorContext
    }

    override def transform(agentId: String, agent: A): (String, String) = {
      // initialize the timer for this story for each population member
      timers.putIfAbsent(agentId, genTimerValue)
      null
    }

    /**
      * Decrement all counters + for any counter currently at 0, trigger the execution of the story + reset the timer
      * */
    def timeStep(): Unit = {

      timers
        .all().asScala
        .foreach { timer =>

          val updatedTimer =
            if (timer.value == 0) {
              context.forward(timer.key, timer.key)
              genTimerValue
            } else {
              timer.value -1
            }

          timers.put(timer.key, updatedTimer)

        }
    }

    /**
      * Generates a new value for this timer
      * */
    def genTimerValue: Int = {
      val timerValue = timerGen.head
      timerGen = timerGen.tail
      timerValue
    }


    // ------------------
    // unused methods


    override def close(): Unit = {}

    override def punctuate(timestamp: Long) = null

  }


}
