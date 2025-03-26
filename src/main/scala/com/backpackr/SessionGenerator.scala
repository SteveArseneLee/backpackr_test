package com.backpackr

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import java.security.MessageDigest

object SessionGenerator {
  private def md5Hash(input: String): String = {
    val md = MessageDigest.getInstance("MD5")
    md.update(input.getBytes("UTF-8"))
    md.digest.map("%02x".format(_)).mkString
  }

  private val md5UDF = udf(md5Hash _)

  /**
   * 사용자별 세션 ID를 생성하는 함수
   * @param df KST 기준으로 정리된 DataFrame (event_time_kst 포함)
   * @return session_id 컬럼이 추가된 DataFrame
   */
  def addSessionColumn(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // 1. 동일 사용자를 기준으로 시간 순으로 정렬
    val userWindow = Window.partitionBy("user_id").orderBy("event_time_kst")

    // 2. 직전 "event_time_kst"와의 차이를 계산한 prev_time 생성
    val withTimeDiff = df.withColumn("prev_time", lag("event_time_kst", 1).over(userWindow))
      .withColumn("time_diff_sec",
        unix_timestamp($"event_time_kst") - unix_timestamp($"prev_time"))

    // 3. 300초 이상 차이나면 새로운 세션
    val withSessionFlag = withTimeDiff.withColumn("is_new_session",
      when($"time_diff_sec".isNull || $"time_diff_sec" >= 300, 1).otherwise(0))

    // 4. 사용자별 session별 그룹 번호 생성
    val withSessionGroup = withSessionFlag.withColumn("session_group",
      sum("is_new_session").over(userWindow.rowsBetween(Window.unboundedPreceding, 0)))

    // 5. (user_id + session_group)에 MD5를 적용해 session_id 생성
    val withSessionId = withSessionGroup
      .withColumn("session_key", concat_ws("_", $"user_id", $"session_group"))
      .withColumn("session_id", md5UDF($"session_key"))
      .drop("prev_time", "time_diff_sec", "is_new_session", "session_group", "session_key")

    withSessionId
  }
}