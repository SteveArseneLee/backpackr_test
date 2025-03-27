package com.backpackr

import org.apache.spark.sql.{DataFrame, SparkSession}

object WAUCalculator {
  /**
   * user_id 기준 WAU 계산
   */
  def calcWAUByUser()(implicit spark: SparkSession): DataFrame = {
    spark.sql(
      """
        |SELECT
        |  weekofyear(event_time_kst) AS week_number,
        |  MIN(DATE(event_time_kst)) AS start_date,
        |  MAX(DATE(event_time_kst)) AS end_date,
        |  COUNT(DISTINCT user_id) AS wau_user_id
        |FROM backpackr_events
        |GROUP BY weekofyear(event_time_kst)
        |ORDER BY week_number
        |""".stripMargin)
  }

  /**
   * session_id 기준 WAU 계산
   */
  def calcWAUBySession()(implicit spark: SparkSession): DataFrame = {
    spark.sql(
      """
        |SELECT
        |  weekofyear(event_time_kst) AS week_number,
        |  MIN(DATE(event_time_kst)) AS start_date,
        |  MAX(DATE(event_time_kst)) AS end_date,
        |  COUNT(DISTINCT session_id) AS wau_session_id
        |FROM backpackr_events
        |GROUP BY weekofyear(event_time_kst)
        |ORDER BY week_number
        |""".stripMargin)
  }
}
