package spark.practise
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

object CorrectingDataTypes {

  private def get_interval(column_name: String) : Column = {
    expr(
      s"""
        |CAST(CONCAT(LEFT(LPAD($column_name, 4, '0'), 2),
        |':',
        |RIGHT(LPAD($column_name, 4, '0'), 2))
        |AS INTERVAL HOUR TO MINUTE)
        |""".stripMargin
    )
  }
  def main(args: Array[String]) : Unit = {
    val spark = Utils.getSparkSession
    import spark.implicits._
    val flight_time_df = spark.table("dev.spark_db.flight_time_raw")

//    val step_1_df = flight_time_df.withColumns(Map(
//      "CRS_DEP_TIME_HH" -> expr("left(lpad(CRS_DEP_TIME, 4, '0'), 2)"),
//      "CRS_DEP_TIME_MM" -> expr("right(lpad(CRS_DEP_TIME, 4, '0'), 2)")
//    ))
//
//    val step_2_df = step_1_df.withColumns(Map(
//      "CRS_DEP_TIME_NEW" -> expr("cast(concat(CRS_DEP_TIME_HH, ':', CRS_DEP_TIME_MM) as interval hour to minute)")
//    ))
    val result_df = flight_time_df.withColumns(Map(
      "CRS_DEP_TIME" -> get_interval("CRS_DEP_TIME"),
      "DEP_TIME" -> get_interval("DEP_TIME"),
      "WHEELS_ON" -> get_interval("WHEELS_ON"),
      "CRS_ARR_TIME" -> get_interval("CRS_ARR_TIME"),
      "ARR_TIME" -> get_interval("ARR_TIME"),
      "TAXI_IN" -> expr("cast(TAXI_IN as INTERVAL MINUTE)")
    ))

    result_df.write
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable("dev.spark_db.flight_time_raw")

    spark.sql("SELECT * FROM dev.spark_db.flight_time_raw").show(10)
  }
}
