package com.backpackr

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("backpackr_proj")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    /**
     * Schema 출력
     */
//    // 2019-Oct.csv 파일 읽고 스키마 출력
//    val octDF = spark.read
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .csv("2019-Oct.csv")
//
//    println("[2019-Oct.csv 스키마] : ")
//    octDF.printSchema()
//
//    // 2019-Nov.csv 파일 읽고 스키마 출력
//    val novDF = spark.read
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .csv("2019-Nov.csv")
//
//    println("[2019-Nov.csv 스키마] : ")
//    novDF.printSchema()

    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("2019-Oct.csv", "2019-Nov.csv")
    rawDF.printSchema()

    val withPartitionDF = PartitionWriter.addKSTPartitionColumn(rawDF)(spark)

    withPartitionDF.select("event_time", "event_time_kst", "partition_date").show(20, truncate = false)
    withPartitionDF.printSchema()


    spark.stop()
  }
}
