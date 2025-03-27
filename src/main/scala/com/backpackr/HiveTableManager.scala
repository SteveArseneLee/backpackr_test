package com.backpackr

import org.apache.spark.sql.SparkSession

object HiveTableManager {

  /**
   * Hive External Table 생성 함수
   * @param externalPath 저장된 Parquet 절대경로
   */
  def createExternalTable(externalPath: String)(implicit spark: SparkSession): Unit = {
    // 테이블이 이미 존재하면 삭제
    spark.sql("DROP TABLE IF EXISTS backpackr_events")

    spark.sql(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS backpackr_events (
         |  event_time TIMESTAMP,
         |  event_type STRING,
         |  product_id INT,
         |  category_id BIGINT,
         |  category_code STRING,
         |  brand STRING,
         |  price DOUBLE,
         |  user_id INT,
         |  user_session STRING,
         |  event_time_kst TIMESTAMP,
         |  session_id STRING
         |)
         |PARTITIONED BY (partition_date STRING)
         |STORED AS PARQUET
         |LOCATION '$externalPath'
         |TBLPROPERTIES ('parquet.compression'='SNAPPY')
         |""".stripMargin)

    // MetaStore Check. 파티션 metadata 동기화
    spark.sql("MSCK REPAIR TABLE backpackr_events")

    // 결과 확인
    println("Hive 테이블 리스트:")
    spark.sql("SHOW TABLES").show(truncate = false)

    println("Hive 테이블 데이터 수:")
    spark.sql("SELECT COUNT(*) AS total FROM backpackr_events").show()
  }
}

