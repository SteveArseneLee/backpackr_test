package com.backpackr

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object PartitionWriter {
  /**
   * event_time(UTC) → event_time_kst 컬럼 추가 후,
   * yyyyMMdd 기준으로 partition_date 컬럼 추가
   */
  def addKSTPartitionColumns(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    df
      .withColumn("event_time_kst", from_utc_timestamp($"event_time", "Asia/Seoul"))
      .withColumn("partition_date", date_format($"event_time_kst", "yyyyMMdd"))
  }
}
