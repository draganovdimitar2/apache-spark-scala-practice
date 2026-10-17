package spark.practice
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, to_date}

object ExampleLocalPractice {
  def main(args: Array[String]): Unit = {
    val spark = Utils.getSparkSession
    import spark.implicits._

    val raw_df = spark.read
      .option("multiline", "true")
      .json("/Volumes/dev/spark_db/datasets/spark_programming/data/date.json")

    raw_df.show(5)
    raw_df.printSchema()

    val transformed_df = raw_df.withColumns(
      Map(
        "event_date" -> expr("to_date(event_date) AS event_date"),
        "hour_to_min_duration" -> expr(
          "CAST(hour_to_min_duration AS INTERVAL HOUR TO MINUTE) AS hour_to_min"
        ),
        "hour_duration" -> expr(
          "CAST(hour_duration AS INTERVAL HOUR) AS hour_duration"
        ),
        "day_to_hour_duration" -> expr(
          "CAST(day_to_hour_duration AS INTERVAL DAY TO HOUR) AS day_to_hour_duration"
        ),
        "day_to_minute_duration" -> expr(
          "CAST(day_to_minute_duration AS INTERVAL DAY TO MINUTE) AS day_to_minute_duration"
        ),
        "day_to_second_duration" -> expr(
          "CAST(day_to_second_duration AS INTERVAL DAY TO SECOND) AS day_to_second_duration"
        ),
        "hour_to_second_duration" -> expr(
          "CAST(hour_to_second_duration AS INTERVAL HOUR TO SECOND) AS hour_to_second_duration"
        ),
        "minute_to_second_duration" -> expr(
          "CAST(minute_to_second_duration AS INTERVAL MINUTE TO SECOND) AS minute_to_second_duration"
        ),
        "second_duration" -> expr(
          "CAST(second_duration AS INTERVAL SECOND) AS second_duration"
        )
      )
    )

    transformed_df.show()
    transformed_df.printSchema()
  }
}
