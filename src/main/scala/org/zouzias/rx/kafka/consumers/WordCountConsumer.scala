package org.zouzias.rx.kafka.consumers

import com.cj.kafka.rx.RxConsumer
import org.zouzias.rx.kafka.utils.RxConsumerImplicits
import org.zouzias.rx.kafka.utils.RxConsumerImplicits._

/**
 * A simple word count example using kafka-rx
 */
object WordCountConsumer extends App{

  // Connect to local kafka cluster
  val consumer = new RxConsumer("localhost:2181", "word-count-consumer")

  // Fetch messages from topic "words"
  val sentences = consumer.getStringStream("words").map(x => x.value)

  // Split messages by whitespace
  val words = sentences.flatMapIterable{
    sentence => sentence.split("\\s+")
  }

  // Initialize empty map
  val empty = Map[String, Int]().withDefaultValue(0)

  // counts is a map that contains words -> word_counts
  val counts = words.scan(empty){
    (counts, word) =>
      val count = counts(word) + 1
      counts + (word -> count)
  }

  // print out the word-count
  counts.foreach(println)
}
