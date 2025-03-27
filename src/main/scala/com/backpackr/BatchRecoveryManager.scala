package com.backpackr

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.net.URI

object BatchRecoveryManager {
  /**
   * 이미 저장된 partition_date 디렉토리를 검사하여,
   * 신규 partition만 Parquet(Snappy) 포맷으로 저장
   *
   * @param df          세션 처리 완료된 DataFrame
   * @param outputPath  저장할 디렉토리 경로
   */
  def saveOnlyNewPartitions(df: DataFrame, outputPath: String)(implicit spark: SparkSession): Unit = {
    val fs = FileSystem.get(new URI(outputPath), spark.sparkContext.hadoopConfiguration)
    val fsPath = new Path(outputPath)

    // 기존 저장된 partition_date 목록 조회
    val existingPartitions: Set[String] = if (fs.exists(fsPath)) {
      fs.listStatus(fsPath)
        .filter(_.isDirectory)
        .map(_.getPath.getName.replace("partition_date=", ""))
        .toSet
    } else Set.empty[String]

    // 아직 저장되지 않은 partition_date만 필터링
    val notSavedDF = df.filter(!df("partition_date").isin(existingPartitions.toSeq: _*))

    // 저장 여부 판단 후 저장 실행
    if (!notSavedDF.isEmpty) {
      println(s"New partition saving: ${notSavedDF.select("partition_date").distinct().collect().mkString(", ")}")
      StorageWriter.writeAsPartitionedParquet(notSavedDF, outputPath)
    } else {
      println("No new partition")
    }
  }
}
