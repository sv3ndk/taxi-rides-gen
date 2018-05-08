package svend.taxirides

import java.util.{Calendar, Properties}

import org.apache.kafka.streams.StreamsConfig

object Config {

  object topics {
    val clientPopulation = "clients-population"
  }

  def kafkaProducerProps= {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    // super brittle config: this is a simulator, we care about speed and don't care if we drop stuff on the floor
    // from time to time
    props.put("acks", "0")
    props.put("retries", "0")

    props
  }

  val kafkaStreamsProps: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application" + Calendar.getInstance.getTimeInMillis)
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    // help deduplicating existing values in a KTable, while keeping some throughput
    p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100)
    p
  }


}