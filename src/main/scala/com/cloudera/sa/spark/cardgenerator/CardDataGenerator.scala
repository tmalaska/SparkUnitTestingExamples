package com.cloudera.sa.spark.cardgenerator

import java.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object CardDataGenerator {
  def main(args:Array[String]): Unit = {

    if (args.length == 0) {
      println("<runLocal> " +
        "<accountTable> " +
        "<cardTable> " +
        "<transTable> " +
        "<numOfAccounts> " +
        "<numOfCards> " +
        "<numOfTrans> " +
        "<numOfPartitionWriters> ")
      return
    }

    val runLocal = args(0).equalsIgnoreCase("l")
    val accountTable = args(1)
    val cardTable = args(2)
    val transTable = args(3)
    val numOfAccounts = args(4).toInt
    val numOfCards = args(5).toInt
    val numOfTrans = args(6).toInt
    val numOfPartitionWriters = args(7).toInt

    val sc: SparkContext = if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local[2]", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val sparkConf = new SparkConf().setAppName("Spark Data Generator")
      new SparkContext(sparkConf)
    }

    val hiveContext = new HiveContext(sc)

    println("-----------------------------")
    println("Generate Account Data")
    println("-----------------------------")
    val numOfAccountWriters = (numOfPartitionWriters * (numOfCards.toDouble/numOfTrans.toDouble)).toInt + 1
    val accountPartitions = sc.parallelize((1 to numOfAccountWriters).toSeq, numOfAccountWriters)
    generateAccountData(accountTable, numOfAccounts, numOfAccountWriters, hiveContext, accountPartitions)

    println("-----------------------------")
    println("Generate Card Data")
    println("-----------------------------")
    val numOfCardWriters = (numOfPartitionWriters * (numOfCards.toDouble/numOfTrans.toDouble)).toInt + 1
    val cardPartitions = sc.parallelize((1 to numOfCardWriters).toSeq, numOfCardWriters)
    generateCardData(cardTable, numOfAccounts, numOfCards, numOfCardWriters, hiveContext, cardPartitions)

    println("-----------------------------")
    println("Generate Tran Data")
    println("-----------------------------")
    val tranPartitions = sc.parallelize((1 to numOfPartitionWriters).toSeq, numOfPartitionWriters)
    generateTranData(transTable, numOfCards, numOfTrans, numOfPartitionWriters, hiveContext, tranPartitions)

  }

  def generateAccountData(accountTable: String, numOfAccounts: Int, numOfPartitionWriters: Int, hiveContext: HiveContext, partitions: RDD[Int]): Unit = {
    val accountRDD = partitions.flatMap(r => {
      val mutableList = new mutable.MutableList[Row]
      val loops = numOfAccounts / numOfPartitionWriters
      val random = new Random()
      for (i <- 0 until loops) {
        mutableList += Row(i.toLong + r.toLong * loops, haiku(random), haiku(random), random.nextInt(120))
      }
      mutableList.toSeq
    })

    hiveContext.sql("create table " + accountTable + " (" +
      "account_id BIGINT," +
      "first_name STRING," +
      "last_name STRING," +
      "age INT)" +
      "stored as parquet ")

    val emptyAccountDF = hiveContext.sql("select * from " + accountTable + " limit 0")
    hiveContext.createDataFrame(accountRDD, emptyAccountDF.schema).registerTempTable("accountTmp")
    hiveContext.sql("insert into " + accountTable + " select * from accountTmp")

    hiveContext.sql("select * from " + accountTable + " limit 100").take(100).foreach(println)
  }

  def generateCardData(cardTable: String, numOfAccounts:Int, numOfCards: Int, numOfPartitionWriters: Int, hiveContext: HiveContext, partitions: RDD[Int]): Unit = {
    val accountRDD = partitions.flatMap(r => {
      val mutableList = new mutable.MutableList[Row]
      val loops = numOfCards / numOfPartitionWriters
      val random = new Random()
      for (i <- 0 until loops) {
        mutableList += Row(i.toLong + r.toLong * loops, random.nextInt(numOfAccounts).toLong, 2000 + random.nextInt(20), random.nextInt(12))
      }
      mutableList.toSeq
    })

    hiveContext.sql("create table " + cardTable + " (" +
      "card_id BIGINT, " +
      "account_id BIGINT, " +
      "exp_year INT, " +
      "exp_month INT)" +
      "stored as parquet ")

    val emptyAccountDF = hiveContext.sql("select * from " + cardTable + " limit 0")
    hiveContext.createDataFrame(accountRDD, emptyAccountDF.schema).registerTempTable("cardTmp")
    hiveContext.sql("insert into " + cardTable + " select * from cardTmp")

    hiveContext.sql("select * from " + cardTable + " limit 100").take(100).foreach(println)
  }

  def generateTranData(transTable: String, numOfCards: Int, numOfTrans:Int, numOfPartitionWriters: Int, hiveContext: HiveContext, partitions: RDD[Int]): Unit = {
    val accountRDD = partitions.flatMap(r => {
      val mutableList = new mutable.MutableList[Row]
      val loops = numOfTrans / numOfPartitionWriters

      val now = System.currentTimeMillis()
      val random = new Random()
      for (i <- 0 until loops) {

        mutableList += Row(i.toLong + r.toLong * loops, random.nextInt(numOfCards).toLong, now + i * 60000l + random.nextInt(1000), random.nextInt(1000), random.nextInt(100000).toLong)
      }
      mutableList.toSeq
    })

    hiveContext.sql("create table " + transTable + " (" +
      "tran_id BIGINT, " +
      "card_id BIGINT, " +
      "time_stamp BIGINT," +
      "amount INT," +
      "merchant_id BIGINT)" +
      "stored as parquet ")

    val emptyAccountDF = hiveContext.sql("select * from " + transTable + " limit 0")
    hiveContext.createDataFrame(accountRDD, emptyAccountDF.schema).registerTempTable("transTmp")
    hiveContext.sql("insert into " + transTable + " select * from transTmp")

    hiveContext.sql("select * from " + transTable + " limit 100").take(100).foreach(println)
  }

  val adjs = List("autumn", "hidden", "bitter", "misty", "silent",
    "reckless", "daunting", "short", "rising", "strong", "timber", "tumbling",
    "silver", "dusty", "celestial", "cosmic", "crescent", "double", "far",
    "terrestrial", "huge", "deep", "epic", "titanic", "mighty", "powerful")

  val nouns = List("waterfall", "river", "breeze", "moon", "rain",
    "wind", "sea", "morning", "snow", "lake", "sunset", "pine", "shadow", "leaf",
    "sequoia", "cedar", "wrath", "blessing", "spirit", "nova", "storm", "burst",
    "giant", "elemental", "throne", "game", "weed", "stone", "apogee", "bang")

  def getRandElt[A](xs: List[A], random:Random): A = xs.apply(random.nextInt(xs.size))

  def getRandNumber(ra: Range, random:Random): String = {
    (ra.head + random.nextInt(ra.end - ra.head)).toString
  }

  def haiku(random: Random): String = {
    val xs = getRandNumber(1000 to 9999, random) :: List(nouns, adjs).map(l => getRandElt(l, random))
    xs.reverse.mkString("-")
  }
}
