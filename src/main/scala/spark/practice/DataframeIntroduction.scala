package spark.practice

import org.apache.spark.sql.functions._

object DataframeIntroduction {
  def main(args: Array[String]): Unit = {
    val spark = Utils.getSparkSession
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS dev.spark_db.sf_fire_calls (
        |  CallNumber INT,
        |  UnitID STRING,
        |  IncidentNumber INT,
        |  CallType STRING,
        |  CallDate DATE,
        |  WatchDate DATE,
        |  CallFinalDisposition STRING,
        |  AvailableDtTm TIMESTAMP,
        |  Address STRING,
        |  City STRING,
        |  Zipcode STRING,
        |  Battalion STRING,
        |  StationArea STRING,
        |  Box STRING,
        |  OriginalPriority STRING,
        |  Priority STRING,
        |  FinalPriority STRING,
        |  ALSUnit BOOLEAN,
        |  CallTypeGroup STRING,
        |  NumAlarms INT,
        |  UnitType STRING,
        |  UnitSequenceInCallDispatch INT,
        |  FirePreventionDistrict STRING,
        |  SupervisorDistrict STRING,
        |  Neighborhood STRING,
        |  Location STRING,
        |  RowID STRING,
        |  Delay DOUBLE
        |)
        |""".stripMargin)

    val rawDf = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Volumes/dev/spark_db/datasets/spark_programming/data/sf-fire-calls.csv")

    val fireDf = rawDf.withColumns(Map(
      "AvailableDtTm" -> to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"),
      "Zipcode" -> expr("cast(Zipcode as string)"),
      "FinalPriority" -> expr("cast(FinalPriority as string)"),
    ))

    fireDf.write.mode("overwrite").saveAsTable("dev.spark_db.sf_fire_calls")

    spark.sql("SELECT COUNT(*) FROM dev.spark_db.sf_fire_calls").show()
  }
}
