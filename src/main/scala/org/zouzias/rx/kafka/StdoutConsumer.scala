package org.zouzias.rx.kafka

import com.cj.kafka.rx.RxConsumer
import kafka.serializer.StringDecoder

/**
 * An echo consumer using kafka-rx
 */
object StdoutConsumer extends App{

  val topicName : String = "words"

  // Connect to local Kafka
  val consumer = new RxConsumer("localhost:2181", "stdout-consumer")

  // Print out message to stdout
  consumer.getRecordStream[String,String](topicName, new StringDecoder(), new StringDecoder())
    .map(x => x.value)  // Message only
    .foreach(x => println(x))   // Print out messages
}
