package com.cloudera.sa.spark.unittest.core

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import scala.collection.mutable

class CoreUnitTest extends FunSuite with
BeforeAndAfterEach with BeforeAndAfterAll{

  @transient var sc: SparkContext = null

  override def beforeAll(): Unit = {

    val envMap = Map[String,String](("Xmx", "512m"))

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.broadcast.compress", "false")
    sparkConfig.set("spark.shuffle.compress", "false")
    sparkConfig.set("spark.shuffle.spill.compress", "false")
    sparkConfig.set("spark.io.compression.codec", "lzf")
    sc = new SparkContext("local[2]", "unit test", sparkConfig)
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  test("Test word count") {
    val quotesRDD = sc.parallelize(Seq("Courage is not simply one of the virtues, but the form of every virtue at the testing point",
      "We have a very active testing community which people don't often think about when you have open source",
      "Program testing can be used to show the presence of bugs, but never to show their absence",
      "Simple systems are not feasible because they require infinite testing",
      "Testing leads to failure, and failure leads to understanding"))

    val wordCountRDD = quotesRDD.flatMap(r => r.split(' ')).
      map(r => (r.toLowerCase, 1)).
      reduceByKey((a,b) => a + b)

    val wordMap = new mutable.HashMap[String, Int]()
    wordCountRDD.take(100).
      foreach{case(word, count) => wordMap.put(word, count)}
    //Note this is better then foreach(r => wordMap.put(r._1, r._2)

    assert(wordMap.get("to").get == 4, "The word count for 'to' should had been 4 but it was " + wordMap.get("to").get)
    assert(wordMap.get("testing").get == 5, "The word count for 'testing' should had been 5 but it was " + wordMap.get("testing").get)
    assert(wordMap.get("is").get == 1, "The word count for 'is' should had been 1 but it was " + wordMap.get("is").get)
  }
}
