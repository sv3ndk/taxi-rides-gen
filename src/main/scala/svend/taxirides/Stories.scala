package svend.taxirides

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{ProcessorContext, PunctuationType}
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder
import org.apache.kafka.streams.state.{KeyValueStore, Stores}

import scala.collection.JavaConverters._

object Stories {


  def buildTrigger[ID, M](storyName: String) = {

    val timerStoreName = s"${storyName}Timers"
    val timerStoreSupplier = Stores.inMemoryKeyValueStore(timerStoreName)

    val timerStoreBuilder = new KeyValueStoreBuilder[Clients.ClientId, Long](
      timerStoreSupplier,
      stringSerde,
      longSerde,
      Time.SYSTEM
    )

    (timerStoreName, timerStoreBuilder, () => new ClientsTrigger[ID, M](timerStoreName))

  }

  /**
    * Trigger of a Story: responsible for maintaining a state with a counter for each population member
    * + decrementing it regularly and triggering story execution when needed
    *
    * ID: type of the id of the population member, typically a string
    * M: type of the population member itself (typically some case class)
    * */
  class ClientsTrigger[ID, M](timerStoreName: String) extends Transformer[ID, M, (ID, ID)] {

    var timers: KeyValueStore[ID, Long] = _
    var context: ProcessorContext = _

    override def init(processorContext: ProcessorContext): Unit = {
      processorContext.schedule(100, PunctuationType.WALL_CLOCK_TIME, (timestamp: Long) => timeStep())
      timers = processorContext.getStateStore(timerStoreName).asInstanceOf[KeyValueStore[ID, Long]]
      context = processorContext
    }

    override def transform(clientId: ID, value: M): (ID, ID) = {
      // initialize the timer for this story for each population member
      timers.putIfAbsent(clientId, resetTimer)
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
              resetTimer
            } else {
              timer.value -1
            }

          timers.put(timer.key, updatedTimer)

        }
    }

    /**
      * Generates a new value for this timer
      * */
    def resetTimer: Int = 10


    // ------------------
    // unused methods


    override def close(): Unit = {}

    override def punctuate(timestamp: Long) = null

  }




}
