package org.apache.spark.streaming

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.streaming.dstream.InputDStream

import scala.collection.mutable.{ArrayBuffer, Queue}
import scala.reflect.ClassTag

class TestableQueueInputDStream[T: ClassTag](
                                              ssc: StreamingContext,
                                              val queue: Queue[RDD[T]],
                                              oneAtATime: Boolean,
                                              defaultRDD: RDD[T]
                                              ) extends InputDStream[T](ssc) {

  override def start() { }

  override def stop() { }

  private def readObject(in: ObjectInputStream): Unit = {
    logWarning("queueStream doesn't support checkpointing")
  }

  private def writeObject(oos: ObjectOutputStream): Unit = {
    logWarning("queueStream doesn't support checkpointing")
  }

  override def compute(validTime: Time): Option[RDD[T]] = {
    val buffer = new ArrayBuffer[RDD[T]]()
    queue.synchronized {
      if (oneAtATime && queue.nonEmpty) {
        buffer += queue.dequeue()
      } else {
        buffer ++= queue
        queue.clear()
      }
    }
    if (buffer.nonEmpty) {
      if (oneAtATime) {
        Some(buffer.head)
      } else {
        Some(new UnionRDD(context.sc, buffer.toSeq))
      }
    } else if (defaultRDD != null) {
      Some(defaultRDD)
    } else {
      Some(ssc.sparkContext.emptyRDD)
    }
  }

}