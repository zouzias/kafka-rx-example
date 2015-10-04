package org.zouzias.rx.kafka.producers

import java.util.Properties

import com.cj.kafka.rx.Record
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/**
 * A simple example writing message to Kafka
 *
 * Produces message to Kafka topic "words"
 */
object SimpleProducer extends App{

  type StringRecord = ProducerRecord[String, String]
  type StringProducer = Producer[String, String]

  val topicName : String = "words"

  // Connect to local Kafka
  val producer = getProducer("localhost:9092")


  val msgs = List("hello", "world", "Tassos", "how are you today?")

  msgs.foreach(x => producer.send(new StringRecord(topicName,x)))

  producer.close()

  /**
   * Return a Kafka producer for string messages
   *
   * @return
   */
  def getProducer(brokerList : String): StringProducer = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerList)
    props.put("key.serializer", classOf[StringSerializer].getCanonicalName)
    props.put("value.serializer", classOf[StringSerializer].getCanonicalName)
    new KafkaProducer[String, String](props)
  }

  /**
   * Format a message
   * @param result
   */
  def formatMessage(result: Record[String, String]) = {
    println(s"Produced: [${result.topic}] - ${result.partition} -> ${result.offset} :: ${result.value}")
  }
}
