package spark.practise

import org.apache.spark.sql.types.FloatType

import java.sql.Date

object CreatingDataFrame {
  private case class Employee(id: Long, name: String, age: Int, date: Date)

  case class Salary(id: Long, salary: Float)

  def main(args: Array[String]): Unit = {
    val spark = Utils.getSparkSession
    import spark.implicits._

    val json_df = spark.read.format("json")
      .load("/Volumes/dev/spark_db/datasets/spark_programming/data/diamonds.json")

    val csv_df = spark.read.format("csv")
      .option("header", "true")
      .load("/Volumes/dev/spark_db/datasets/spark_programming/data/booking-summary.csv")

    val sql_df = spark.sql(
      """
        |SELECT cut, sum(price) FROM dev.spark_db.diamonds GROUP BY cut""".stripMargin)

    val scalaCollectionDf = Seq(
      Employee(1, "Ivan", 25, Date.valueOf("2018-01-01")),
      Employee(2, "Boyan", 35, Date.valueOf("2018-01-01")),
      Employee(3, "Dragan", 45, Date.valueOf("2018-01-01"))
    ).toDF("id", "name", "age", "date")

    val salaryCollectionDf = Seq(
      Salary(1, 0.1f),
      Salary(1, 0.2f),
      Salary(2, 1000.0f)
    ).toDF()


    salaryCollectionDf
      .groupBy("id")
      .sum("salary")
      .show()
  }
}
