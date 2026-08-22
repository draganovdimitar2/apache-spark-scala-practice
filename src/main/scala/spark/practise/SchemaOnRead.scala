package spark.practise
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object SchemaOnRead {

  def main(arr: Array[String]): Unit = {
    val spark = Utils.getSparkSession
    import spark.implicits._

    val raw_df = spark.read
      .format("json")
      .option("dateFormat", "M/d/yyyy")
      .option("mode", "FAILFAST")
      .load("/Volumes/dev/spark_db/datasets/spark_programming/data/flight-time.json")

    raw_df.printSchema()

    val flight_schema = StructType(Array(
      StructField("FL_DATE", DateType),
      StructField("OP_CARRIER", StringType),
      StructField("OP_CARRIER_FL_NUM", StringType),
      StructField("ORIGIN", StringType),
      StructField("ORIGIN_CITY_NAME", StringType),
      StructField("DEST", StringType),
      StructField("DEST_CITY_NAME", StringType),
      StructField("CRS_DEP_TIME", LongType),
      StructField("DEP_TIME", LongType),
      StructField("WHEELS_ON", IntegerType),
      StructField("TAXI_IN", IntegerType),
      StructField("CRS_ARR_TIME", LongType),
      StructField("ARR_TIME", LongType),
      StructField("CANCELLED", IntegerType),
      StructField("DISTANCE", IntegerType)
    ))

    val raw_df_with_schema = spark.read
      .format("json")
      .option("dateFormat", "M/d/yyyy")
      .option("mode", "FAILFAST")
      .schema(flight_schema)
      .load("/Volumes/dev/spark_db/datasets/spark_programming/data/flight-time.json")

    raw_df_with_schema.write
      .mode("overwrite")
      .saveAsTable("dev.spark_db.flight_time_raw")

    spark.sql("""SELECT * FROM dev.spark_db.flight_time_raw""").show()
    //    INPUT SCHEMA:
//    root
//    |-- ARR_TIME: long (nullable = true)
//    |-- CANCELLED: long (nullable = true)
//    |-- CRS_ARR_TIME: long (nullable = true)
//    |-- CRS_DEP_TIME: long (nullable = true)
//    |-- DEP_TIME: long (nullable = true)
//    |-- DEST: string (nullable = true)
//    |-- DEST_CITY_NAME: string (nullable = true)
//    |-- DISTANCE: long (nullable = true)
//    |-- FL_DATE: string (nullable = true)
//    |-- OP_CARRIER: string (nullable = true)
//    |-- OP_CARRIER_FL_NUM: long (nullable = true)
//    |-- ORIGIN: string (nullable = true)
//    |-- ORIGIN_CITY_NAME: string (nullable = true)
//    |-- TAXI_IN: long (nullable = true)
//    |-- WHEELS_ON: long (nullable = true)
//     Problem to solve: Schema and order of columns is not matching the target table
  }
}
