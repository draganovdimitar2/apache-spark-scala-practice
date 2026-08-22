package spark.practise

import org.apache.spark.sql.connect.SparkSession
import org.apache.spark.sql.functions._

object Transformations {
  def main(args: Array[String]): Unit = {
    val spark = Utils.getSparkSession

    val df = spark.read.table("dev.spark_db.diamonds")
      .filter(col("price")>600)
      .groupBy(col("cut"))
      .count()

    df.show()
  }

}
