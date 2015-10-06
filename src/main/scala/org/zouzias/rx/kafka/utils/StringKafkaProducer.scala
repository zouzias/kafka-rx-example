package org.zouzias.rx.kafka.utils

import java.util.Properties

import com.cj.kafka.rx.Record
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringSerializer

/**
 * String Kafka producer
 */
object StringKafkaProducer {

  /**
   * Return a Kafka producer for string messages
   *
   * @return
   */
  def apply(brokerList : String): KafkaProducer[String, String] = {
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
