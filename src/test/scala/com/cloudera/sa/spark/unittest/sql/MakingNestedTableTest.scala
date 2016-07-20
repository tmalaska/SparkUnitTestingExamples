package com.cloudera.sa.spark.unittest.sql

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

object MakingNestedTableTest   extends FunSuite with
  BeforeAndAfterEach with BeforeAndAfterAll {

  @transient var sc: SparkContext = null
  @transient var hiveContext: HiveContext = null

  override def beforeAll(): Unit = {

    val envMap = Map[String, String](("Xmx", "512m"))

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.broadcast.compress", "false")
    sparkConfig.set("spark.shuffle.compress", "false")
    sparkConfig.set("spark.shuffle.spill.compress", "false")
    sparkConfig.set("spark.io.compression.codec", "lzf")
    sc = new SparkContext("local[2]", "unit test", sparkConfig)
    hiveContext = new HiveContext(sc)
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  test("Test table creation and summing of counts") {

    val loanRDD = sc.parallelize(Seq(Row("100", "100000000"),
                                      Row("101", "100000000"),
                                      Row("102", "100000000")))

    val partiesRDD = sc.parallelize(Seq(Row("100", "ted"),
      Row("101", "bob", "42"),
      Row("101", "cat", "42"),
      Row("102", "Jen", "42"),
      Row("102", "Jenny", "42"),
      Row("102", "Ed", "42")))

    //loan
    hiveContext.sql("create table loan (id string, amount string) as parquet")
    val emptyLoanDF = hiveContext.sql("select * from loan limit 0;")
    val loanDF = hiveContext.createDataFrame(loanRDD, emptyLoanDF.schema)
    loanDF.registerTempTable("loanTmp")
    hiveContext.sql("insert into loan select * from loanTmp")

    //parties
    hiveContext.sql("create table party (loan_id string, name string, age string) as parquet")
    val emptyPartyDF = hiveContext.sql("select * from party limit 0;")
    val partyDF = hiveContext.createDataFrame(partiesRDD, emptyPartyDF.schema)
    partyDF.registerTempTable("partyTmp")
    hiveContext.sql("insert into party select * from partyTmp")

    val keyValueParty = hiveContext.sql("select * from party").map(r => {
      //Key Value
      (r.getString(r.fieldIndex("loan_id")), Seq(r))
    }).reduceByKey((a, b) => {
      a ++ b
    })

    val keyValueLoan = hiveContext.sql("select * from loan").map(r => {
      //Key Value
      (r.getString(r.fieldIndex("id")), r.getString(r.fieldIndex("amount")))
    })

    val nestedRDD = keyValueLoan.join(keyValueParty).map(r => {
      val loanId = r._1
      val loanAmount = r._2._1
      val seqOfParties = r._2._2.map(r => {
        Row(r.getString(r.fieldIndex("name")),
        r.getString(r.fieldIndex("age")))
      })

      Row(loanId, loanAmount, seqOfParties)
    })

    hiveContext.sql("create table nested (" +
      "loan_id string, " +
      "amount string, " +
      "party <array<struct<" +
      "  name: String," +
      "  age: String>>" +
      ") as parquet")

    val emptyNestedDF = hiveContext.sql("select * from nested limit 0;")
    val nestedDF = hiveContext.createDataFrame(nestedRDD, emptyNestedDF.schema)
    nestedDF.registerTempTable("nestedTmp")
    hiveContext.sql("insert into nested select * from nestedTmp")


  }
}
