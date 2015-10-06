package org.zouzias.rx.kafka.pipes

import com.cj.kafka.rx.RxConsumer
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.ProducerRecord
import org.zouzias.rx.kafka.utils.StringKafkaProducer

import org.zouzias.rx.kafka.utils.RxConsumerImplicits._

/**
 * A simple pipe from sourceTopic to TargetTopic
 *
 * Read messages from "sourceTopic" and writes them to "targetTopic"
 */
object SimplePipe extends App{

  type StringRecord = ProducerRecord[String, String]

  val producer = StringKafkaProducer("localhost:9092")

  val sourceTopic : String = "words"
  val targetTopic : String = "new-words"

  val consumer = new RxConsumer("localhost:2181", "simple-pipe")

  // pipe sourceTopic -> targetTopic
  consumer.getStringStream(sourceTopic)
    .map(_.value).foreach(x => producer.send(new StringRecord(targetTopic, x)))


  // Block for ever?
  readLine()
}
