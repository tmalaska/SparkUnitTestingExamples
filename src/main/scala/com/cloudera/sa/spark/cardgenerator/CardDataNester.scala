package com.cloudera.sa.spark.cardgenerator

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object CardDataNester {
  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("<runLocal> " +
        "<accountTable> " +
        "<cardTable> " +
        "<transTable> " +
        "<nestedTableName> ")
      return
    }

    val runLocal = args(0).equalsIgnoreCase("l")
    val accountTable = args(1)
    val cardTable = args(2)
    val transTable = args(3)
    val nestedTableName = args(4)

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

    val hc = new HiveContext(sc)

    val transTableDF = hc.sql("select * from " + transTable)

    val transGroupByRDD = transTableDF.map(r => {
      (r.getLong(r.fieldIndex("card_id")), r)
    }).groupByKey()

    val cardTableDF = hc.sql("select * from " + cardTable)

    val nestedCardRDD = cardTableDF.map(r => {
      (r.getLong(r.fieldIndex("card_id")), r)
    }).join(transGroupByRDD).map(r => {
      val card = r._2._1
      val trans = r._2._2.map(t => {
        Row(
          t.getLong(t.fieldIndex("tran_id")),
          t.getLong(t.fieldIndex("time_stamp")),
          t.getInt(t.fieldIndex("amount")),
          t.getLong(t.fieldIndex("merchant_id")))
      })

      (card.getLong(card.fieldIndex("account_id")),
      Row(
        card.getLong(card.fieldIndex("card_id")),
        card.getInt(card.fieldIndex("exp_year")),
        card.getInt(card.fieldIndex("exp_month")),
        trans))
    }).groupByKey()

    val accountTableDF = hc.sql("select * from " + accountTable)

    val nestedAccountRdd = accountTableDF.map(r => {
      (r.getLong(r.fieldIndex("account_id")), r)
    }).join(nestedCardRDD).map(r => {
      val account = r._2._1
      Row(
        account.getLong(account.fieldIndex("account_id")),
        account.getString(account.fieldIndex("first_name")),
        account.getString(account.fieldIndex("last_name")),
        account.getInt(account.fieldIndex("age")),
        r._2._2.toSeq
      )
    })

    hc.sql("create table " + nestedTableName + "(" +
      " account_id BIGINT," +
      " first_name STRING," +
      " last_name STRING," +
      " age INT," +
      " card ARRAY<STRUCT<" +
      "   card_id: BIGINT," +
      "   exp_year: INT," +
      "   exp_month: INT," +
      "   tran: ARRAY<STRUCT<" +
      "     tran_id: BIGINT," +
      "     time_stamp: BIGINT," +
      "     amount: INT," +
      "     merchant_id: BIGINT" +
      "   >>" +
      " >>" +
      ") stored as parquet")

    val emptyNestedDf = hc.sql("select * from " + nestedTableName + " limit 0")

    hc.createDataFrame(nestedAccountRdd, emptyNestedDf.schema).registerTempTable("nestedTmp")

    hc.sql("insert into " + nestedTableName + " select * from nestedTmp")

    sc.stop()
  }
}
