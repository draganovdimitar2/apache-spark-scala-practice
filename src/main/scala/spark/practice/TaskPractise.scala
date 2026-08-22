package spark.practice

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object TaskPractise {
  def main(args: Array[String]): Unit = {
    val spark = Utils.getSparkSession
    import spark.implicits._

    val data = Seq(
      (1, 101, 50, "completed"),
      (2, 101, 30, "cancelled"),
      (3, 102, 100, "completed"),
      (4, 103, 70, "completed"),
      (5, 102, 20, "completed"),
    )

    val orders = data.toDF("order_id", "user_id", "price", "status")

    // Task 1 — Basic transformation 
    val totalSpendDf = orders
      .filter(col("status") === "completed")
      .groupBy("user_id")
      .agg(sum("price").alias("total_spend"))


    // Task 3 — Data cleaning 
    val data_2 = Seq(
      (1, "John", "John@test.com"),
      (2, null, "bob@test.com"),
      (3, "Anna", null),
      (4, "Mike", "mike@test.com"),
      (4, "Mike", "mike@test.com")
    )

    val customers = data_2.toDF("id", "name", "email")

    val customers_df = customers
      .select("id", "name", "email")
      .distinct()
      .filter(col("email").isNotNull)

    customers_df.withColumn("name", when(col("name").isNull, lit("Unknown")).otherwise(col("name"))).show()

    // Task 4 — Date transformation 
    val events_df1 = Seq(
      (1, "2026-07-01 14:35:00"),
      (2, "2026-07-02 10:15:00")
    ).toDF("id", "timestamp")

    val events_df2 = events_df1.select("id", "timestamp")
    val events_df3 = events_df2.withColumn("date", date_format($"timestamp", "yyyy-MM-dd"))
    val events_df4 = events_df3.withColumn("hour", hour($"timestamp")).drop("timestamp").show()

    // Task 2 — Window functions 
    val employees = Seq(
      ("John", "IT", 5000),
      ("Anna", "IT", 7000),
      ("Mark", "HR", 4000),
      ("Lisa", "HR", 6000),
      ("Bob",  "IT", 6000)
    ).toDF("name", "department", "salary")

    val employeesDf = employees.withColumn("rank", dense_rank().over(Window.partitionBy($"department").orderBy($"salary"))).show()
  }
}
