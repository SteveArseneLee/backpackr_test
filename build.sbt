ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "backpackr_proj",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.5",
      "org.apache.spark" %% "spark-sql" % "3.5.5",
      "org.apache.spark" %% "spark-hive" % "3.5.5"
    )
  )
