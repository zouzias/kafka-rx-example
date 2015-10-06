package org.zouzias.rx.kafka.consumers

import com.cj.kafka.rx.RxConsumer
import kafka.serializer.StringDecoder

/**
 *  Consumes Kafka topics to stdout
 *
 * Read messages from topic "words" and print them to stdout
 *
 * Assumes that messages are serialized using StringDecoder/StringEncoder.
 */
object StdoutConsumer extends App{

  val topicName : String = "new-words"
  val consumer = new RxConsumer("localhost:2181", "stdout-consumer")

  // Print out message to stdout
  consumer.getRecordStream[String,String](topicName, new StringDecoder(), new StringDecoder())
    .map(x => x.value)  // Message only
    .foreach(x => println(x))   // Print out messages
}
