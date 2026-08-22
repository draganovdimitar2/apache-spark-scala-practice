package spark.practice
import org.apache.spark.sql.types._

import java.sql.Date


object EdaTypeCorrection {

  def main(args: Array[String]) : Unit = {
    val spark = Utils.getSparkSession
    import spark.implicits._

    val schema =
      """
        |id long,
        |name string,
        |dop string,
        |phone long,
        |amount string,
        |discount string
        |""".stripMargin

    val raw_df = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load("/Volumes/dev/spark_db/datasets/spark_programming/data/sales_sample.csv")

    val transformed_df = raw_df.selectExpr(
      "cast(id as string) as transaction_id",
      "name as customer_name",
      "nvl(try_cast(dop as date), to_date(dop, 'dd-MM-yyyy')) as date_of_purchase",
      "phone as customer_phone",
      "cast(amount as long) as purchase_amount",
      "nvl(try_cast(discount as double), 0) as applied_discount"
    ).filter("purchase_amount is not null")
    raw_df.show()
    transformed_df.show()
    transformed_df.describe().show()
  }
}
