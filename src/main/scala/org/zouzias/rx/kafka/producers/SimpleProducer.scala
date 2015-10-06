package org.zouzias.rx.kafka.producers

import org.apache.kafka.clients.producer.ProducerRecord
import org.zouzias.rx.kafka.utils.StringKafkaProducer

/**
 * A simple example writing message to Kafka
 *
 * Produces message to Kafka topic "words"
 */
object SimpleProducer extends App{

  type StringRecord = ProducerRecord[String, String]

  val topicName : String = "words"

  println("Initialize Simple Producer")

  val producer = StringKafkaProducer("localhost:9092")
  val messages = List("hello", "world", "Tassos", "how are you today?", "again")
  messages.foreach(x => producer.send(new StringRecord(topicName,x)))

  producer.close()
}
