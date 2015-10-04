package org.zouzias.rx.kafka

import com.cj.kafka.rx.{Record, RxConsumer}
import kafka.serializer.StringDecoder
import rx.lang.scala.Observable

/**
 * Consumer for string messages
 */
class RxStringConsumer(val consumer : RxConsumer) {


  def getStringStream(topic : String) : Observable[Record[String, String]] ={
    consumer.getRecordStream[String, String](topic, new StringDecoder(), new StringDecoder())
  }

}

/**
 *
 */
object RxConsumerImplicits {
  implicit def rxConsumerToRxStringConsumer( consumer: RxConsumer) = new RxStringConsumer(consumer)
}
