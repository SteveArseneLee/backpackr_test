package com.backpackr

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object StorageWriter {
  /**
   * parquet + snappy 포맷으로 데이터를 저장하는 함수
   * @param df 저장할 DataFrame
   * @param outputPath 저장 경로
   */
  def writeAsPartitionedParquet(df: DataFrame, outputPath: String)(implicit spark: SparkSession): Unit = {
    df.write
      .format("parquet")
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .partitionBy("partition_date")
      .save(outputPath)

    println(s"저장 완료: $outputPath")
  }
}