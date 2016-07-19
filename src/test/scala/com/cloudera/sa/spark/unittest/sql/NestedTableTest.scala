package com.cloudera.sa.spark.unittest.sql

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class NestedTableTest  extends FunSuite with
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
    /*
    {
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
}
     */

    val jsonRDD = sc.parallelize(Seq("{\"id\": \"0001\",\"type\": \"donut\",\"name\": \"Cake\",\"ppu\": 0.55,\"batters\":{\"batter\":[{ \"id\": \"1001\", \"type\": \"Regular\" },{ \"id\": \"1002\", \"type\": \"Chocolate\" },{ \"id\": \"1003\", \"type\": \"Blueberry\" },{ \"id\": \"1004\", \"type\": \"Devil's Food\" }]},\"topping\":[{ \"id\": \"5001\", \"type\": \"None\" },{ \"id\": \"5002\", \"type\": \"Glazed\" },{ \"id\": \"5005\", \"type\": \"Sugar\" },{ \"id\": \"5007\", \"type\": \"Powdered Sugar\" },{ \"id\": \"5006\", \"type\": \"Chocolate with Sprinkles\" },{ \"id\": \"5003\", \"type\": \"Chocolate\" },{ \"id\": \"5004\", \"type\": \"Maple\" }]}"))

    val jsonDF = hiveContext.read.json(jsonRDD)

    jsonDF.foreach(row => {
      println(row)
    })

    jsonDF.write.parquet("./parquet")

    hiveContext.createExternalTable("jsonNestedTable", "./parquet")

    println(jsonDF.schema)

    hiveContext.sql("select * from jsonNestedTable").foreach(row => {
      println(row)
    })

  }
}
