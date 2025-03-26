package com.backpackr

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder()
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
    // Step 1. KST 기준 daily partition 처리
    // 1. 데이터 로드 (Oct + Nov)

    // 🔹 Spark SQL 표현식 사용을 위한 임포트
    import spark.implicits._

    // 1. Oct + Nov 두 파일을 한번에 읽기
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Seq("2019-Oct.csv", "2019-Nov.csv"): _*) // 리스트로 여러 파일 읽기

    // Step 1: KST 기준 daily partition 처리
    val partitionedDF = PartitionWriter.addKSTPartitionColumns(df)(spark)
//    partitionedDF.select("*").show(5, truncate = false)
//    // 3. 결과 확인 (특정 날짜만 확인)
//    partitionedDF
//      .filter($"partition_date" === "20191002")
//      .select("event_time", "event_time_kst", "partition_date")
//      .show(20, truncate = false)

    // 4. 전체 row 수 확인
    val totalCount = partitionedDF.count()
    println(s"✅ 전체 row 수: $totalCount")

    // Step 2: 동일 user_id내에서 event_time 간격이 5분 이상인 경우 세션 종료로 간주하고 새로운 세션 ID를 생성
    val sessionizedDF = SessionGenerator.addSessionColumn(partitionedDF)

    // 확인용 출력
    sessionizedDF.select("user_id", "event_time", "event_time_kst", "session_id")
      .show(20, truncate = false)

    // Step 3 : 재처리 후 parquet, snappy 처리
//    StorageWriter.writeAsPartitionedParquet(sessionizedDF, "output/backpackr_parquet-snappy")

    spark.stop()
  }
}
