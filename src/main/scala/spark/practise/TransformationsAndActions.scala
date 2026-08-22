package spark.practise

object TransformationsAndActions {
  def main(args: Array[String]): Unit = {
    val spark = Utils.getSparkSession
    import spark.implicits._
    //    SQL variant
    //    spark.sql(
    //      """SELECT  CallType, Zipcode, COUNT(*) as count
    //        |FROM dev.spark_db.sf_fire_calls
    //        |GROUP BY CallType, Zipcode
    //        |ORDER BY count DESC""".stripMargin).show(3) // show(3) or LIMIT 3
    val fire_df = spark.read.table("dev.spark_db.sf_fire_calls")
    val df_1 = fire_df.select("CallType", "Zipcode")
    val df_2 = df_1.where("CallType is not null")
    val df_3 = df_2.groupBy("CallType", "Zipcode").count()
    val df_4 = df_3.orderBy($"count".desc)
    val df_5 = df_4.limit(3)  // or just show(3) instead of limit(3)
    df_5.explain(mode="extended")
  }
}

// Output
//+----------------+-------+-----+
//|        CallType|Zipcode|count|
//+----------------+-------+-----+
//|Medical Incident|  94102|16130|
//|Medical Incident|  94103|14775|
//|Medical Incident|  94110| 9995|
//+----------------+-------+-----+