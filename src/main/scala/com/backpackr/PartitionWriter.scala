package com.backpackr

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * KST 기준 partition_date column 추가
 * @param df : 원본 DataFrame (event_time : timestamp 포함)
 * @return partition_date : column 추가된 DataFrame
 */

object PartitionWriter {
   def addKSTPartitionColumn(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
     import spark.implicits._

     df.withColumn("event_time_ts", to_timestamp($"event_time"))
       .withColumn("event_time_kst", expr("event_time_ts + INTERVAL 9 HOURS"))
       .withColumn("partition_date", date_format($"event_time_kst", "yyyyMMdd"))
       .drop("event_time_ts")
   }
}
