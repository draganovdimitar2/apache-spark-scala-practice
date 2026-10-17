package spark.practice
import org.apache.spark.sql.functions.{
  to_date,
  expr,
  lit,
  array,
  typedLit,
  concat
}

object ColumnExpressions {

  def main(args: Array[String]): Unit = {
    val spark = Utils.getSparkSession
    import spark.implicits._

    /*
        Read data from flight_time and transform as below
            1. Rename fl_date to dep_date
            2. Compute arr_date
            3. Following fields to represent full timestamp
            1. crs_dep_time
            2. dep_time
            3. crs_arr_time
            4. arr_time
     */

    // Do it using SQL
    val flight_time_1_df = spark.sql(
      """
            |SELECT
            |  fl_date AS dep_date,
            |  to_date(fl_date + crs_dep_time + wheels_on + taxi_in) AS arr_date,
            |  dep_date + crs_dep_time AS crs_dep_timestamp,
            |  dep_date + dep_time AS dep_timestamp,
            |  arr_date + crs_arr_time AS crs_arr_timestamp,
            |  arr_date + arr_time AS arr_timestamp
            |FROM dev.spark_db.flight_time
            |WHERE op_carrier = 'DL'
            |""".stripMargin
    )

    // flight_time_1_df.show(5)
    // flight_time_1_df.printSchema()
    // Do it using selectExpr

    val flight_time_raw_df =
      spark.read.table("dev.spark_db.flight_time")

    val flight_time_select_expr = flight_time_raw_df
      .selectExpr(
        "fl_date AS dep_date",
        "to_date(fl_date + crs_dep_time + wheels_on + taxi_in) AS arr_date",
        "dep_date + crs_dep_time AS crs_dep_timestamp",
        "dep_date + dep_time AS dep_timestamp",
        "arr_date + crs_arr_time AS crs_arr_timestamp",
        "arr_date + arr_time AS arr_timestamp"
      )
      .filter("op_carrier == 'DL'")
    // flight_time_select_expr.show(5)
    // flight_time_select_expr.printSchema()

    // Do it using withColumn() or withColumns()

    val flight_time_with_columns = flight_time_raw_df
      .withColumnRenamed("fl_date", "dep_date")
      .withColumn(
        "arr_date",
        expr("to_date(dep_date + crs_dep_time + wheels_on + taxi_in)")
      )
      .withColumns(
        Map(
          "crs_dep_timestamp" -> expr("dep_date + crs_dep_time"),
          "dep_timestamp" -> expr("dep_date + dep_time"),
          "crs_arr_timestamp" -> expr("arr_date + crs_arr_time")
        )
      )
      .filter("op_carrier == 'DL'")

    // flight_time_with_columns.show(5)

    // Do it using column expression

    val flight_time_with_columns_expr = flight_time_raw_df
      .withColumnRenamed("fl_date", "dep_date")
      .withColumn(
        "arr_date",
        to_date($"dep_date" + $"crs_dep_time" + $"wheels_on" + $"taxi_in")
      )
      .withColumns(
        Map(
          "crs_dep_timestamp" -> ($"dep_date" + $"crs_dep_time"),
          "dep_timestamp" -> ($"dep_date" + $"dep_time"),
          "crs_arr_timestamp" -> ($"arr_date" + $"crs_arr_time")
        )
      )
      .filter("op_carrier == 'DL'")

    // flight_time_with_columns_expr.show(5)

  }
}
