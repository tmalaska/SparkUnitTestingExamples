package com.cloudera.sa.spark.unittest.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import scala.collection.mutable.Queue

class StreamingUnitTest extends FunSuite with
BeforeAndAfterEach with BeforeAndAfterAll{

  @transient var sc: SparkContext = null
  @transient var ssc: StreamingContext = null

  override def beforeAll(): Unit = {

    val envMap = Map[String,String](("Xmx", "512m"))

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.broadcast.compress", "false")
    sparkConfig.set("spark.shuffle.compress", "false")
    sparkConfig.set("spark.shuffle.spill.compress", "false")
    sparkConfig.set("spark.io.compression.codec", "lzf")
    sc = new SparkContext("local[2]", "unit test", sparkConfig)
    ssc = new StreamingContext(sc, Milliseconds(200))
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  test("Streaming word count") {

    val firstBatchRDD = sc.parallelize(Seq("a", "b", "c"))
    val secondBatchRDD = sc.parallelize(Seq("a", "e"))
    val thirdBatchRDD = sc.parallelize(Seq("b", "c", "e", "f"))
    val forthBatchRDD = sc.parallelize(Seq("a", "e"))

    val queue = new Queue[RDD[String]]

    queue.+=(firstBatchRDD)
    queue.+=(secondBatchRDD)
    queue.+=(thirdBatchRDD)
    queue.+=(forthBatchRDD)

    println(queue)

    val startTime = System.currentTimeMillis()

    val dstream = new TestableQueueInputDStream(ssc, queue, true, sc.makeRDD(Seq[String](), 1))
    //ssc.queueStream(queue)

    dstream.checkpoint(Seconds(100))

    val batchTotals:DStream[(String, Int)] = dstream.map(r => (r, 1)).reduceByKey(_ + _)

    val streamTotals = batchTotals.updateStateByKey(
      (seq:Seq[Int], opt:Option[Int]) => {
        if (!seq.isEmpty) {
          val totalCountForNew = seq.reduce(_ + _)
          if (opt.isEmpty) {
            Option(totalCountForNew)
          } else {
            Option(opt.get + totalCountForNew)
          }
        } else {
          opt
        }
    })

    streamTotals.foreachRDD(rdd => {

    })

    ssc.checkpoint("./tmp")
    ssc.start()
    ssc.awaitTerminationOrTimeout(2000)

    val endTime = System.currentTimeMillis()

    val rddList = streamTotals.slice(new Time(startTime), new Time(endTime))

    rddList(0).collect().foreach(println)
    assert(rddList(0).collect().filter(r => r._1.equals("a"))(0)._2 == 1)
    rddList(1).collect().foreach(println)
    assert(rddList(1).collect().filter(r => r._1.equals("a"))(0)._2  == 2)
    rddList(2).collect().foreach(println)
    assert(rddList(2).collect().filter(r => r._1.equals("a"))(0)._2  == 2)
    rddList(3).collect().foreach(println)
    assert(rddList(3).collect().filter(r => r._1.equals("a"))(0)._2  == 3)
  }
}
