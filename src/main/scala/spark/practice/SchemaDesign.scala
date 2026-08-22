package spark.practice
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object SchemaDesign {
  def main(args: Array[String]): Unit = {
    val spark = Utils.getSparkSession
    import spark.implicits._

    val raw_df_schema = StructType(Seq(
      StructField("Year", IntegerType),
      StructField("Age", IntegerType),
      StructField("Ethnic", IntegerType),
      StructField("Sex", IntegerType),
      StructField("Area", IntegerType),
      StructField("count", LongType),

    ))

    val raw_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .schema(raw_df_schema)
      .load("/Volumes/dev/spark_db/datasets/spark_programming/data/Data8317.csv")

    raw_df.show()
    raw_df.printSchema()
  }
}
