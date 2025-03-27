package com.backpackr

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder()
      .appName("backpackr_proj")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    /**
     * Schema 출력
     */
    // 2019-Oct.csv 파일 읽고 스키마 출력
    val octDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("2019-Oct.csv")

    println("[2019-Oct.csv 스키마] : ")
    octDF.printSchema()

    // 2019-Nov.csv 파일 읽고 스키마 출력
    val novDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("2019-Nov.csv")

    println("[2019-Nov.csv 스키마] : ")
    novDF.printSchema()

    // Oct + Nov 두 파일을 한번에 읽기
    val rawDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Seq("2019-Oct.csv", "2019-Nov.csv"): _*) // 리스트로 여러 파일 읽기

    // Step 1: KST 기준 daily partition 처리
    val partitionedDF = PartitionWriter.addKSTPartitionColumns(rawDf)

    // 1.1. 확인용 출력
    partitionedDF.select("*").show(5, truncate = false)

    partitionedDF
      .filter($"partition_date" === "20191002")
      .select("event_time", "event_time_kst", "partition_date")
      .show(20, truncate = false)

    val totalCount = partitionedDF.count()
    println(s"전체 row 수: $totalCount")

    // ----------------------------------------
    // Step 2: 동일 user_id내에서 event_time 간격이 5분 이상인 경우 세션 종료로 간주하고 새로운 세션 ID를 생성
    val sessionizedDF = SessionGenerator.addSessionColumn(partitionedDF)

    // 2.1. 확인용 출력
    sessionizedDF.select("user_id", "event_time", "event_time_kst", "session_id")
      .show(5, truncate = false)

    // ----------------------------------------
    // Step 5 : 배치 장애시 복구를 위한 장치 구현
    // "Step 3 : 재처리 후 parquet, snappy 처리"는 Step 5 내부에 구현

    val outputPath = "file:///home/steve/IdeaProjects/backpackr_proj/output/backpackr_parquet-snappy"

    BatchRecoveryManager.saveOnlyNewPartitions(sessionizedDF, outputPath)

    // ----------------------------------------
    // Step 4 : External Table 방식으로 설계 하고, 추가 기간 처리에 대응가능하도록 구현
    HiveTableManager.createExternalTable(outputPath)

    // ----------------------------------------
    // Step 6: WAU 계산 (user_id / session_id 기준)
    val wauByUser = WAUCalculator.calcWAUByUser()
    println("WAU (user_id 기준):")
    wauByUser.show(truncate=false)

    val wauBySession = WAUCalculator.calcWAUBySession()
    println("WAU (session_id 기준):")
    wauBySession.show(truncate=false)
    spark.stop()
  }
}
