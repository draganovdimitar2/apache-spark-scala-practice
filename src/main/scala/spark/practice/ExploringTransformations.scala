package spark.practice

import org.apache.spark.sql.functions.{col, desc, expr, sum, avg, max, min, year, weekofyear}

object ExploringTransformations {

  def main(args: Array[String]): Unit = {

    val spark = Utils.getSparkSession
    import spark.implicits._

    //Answer the following questions using sf_fire_calls table
    val fire_df = spark.read.table("dev.spark_db.sf_fire_calls")

    /*
    Q1. How many distinct types of calls were made to the Fire Department?
      select count(distinct CallType) as distinct_call_type_count
      from dev.spark_db.sf_fire_calls
     */
    println("Q1 Result:")
    val q1_df = fire_df.selectExpr("count(distinct CallType) as distinct_call_type_count")
    q1_df.show(truncate = false)

    /*
    Q2. What were distinct types of calls made to the Fire Department?
      select distinct CallType as distinct_call_types
      from dev.spark_db.sf_fire_calls
      where CallType is not null
     */
    println("Q2 Result:")
    val q2_df = fire_df.selectExpr("CallType as distinct_call_types").distinct()
      .where("distinct_call_types is not null")
    q2_df.show()

    /*
    Q3. Find out all response for delayed times greater than 5 mins?
      select CallNumber, Delay
      from dev.spark_db.sf_fire_calls
      where Delay > 5
     */
    println("\nQ3 Result:")
    val q3_df = fire_df.select("CallNumber", "Delay").where("Delay > 5")
    q3_df.show()

    /*
    Q4. What were the most common call types?
      select CallType, count(*) as count
      from dev.spark_db.sf_fire_calls
      where CallType is not null
      group by CallType
      order by count desc
     */
    println("\nQ4 Result:")
    val q4_df = fire_df
      .filter(col("CallType").isNotNull)
      .groupBy("CallType")
      .count()
      .orderBy(desc("count"))
    q4_df.show()
    /*
    Q5. What zip codes accounted for most common calls?
      select CallType, ZipCode, count(*) as count
      from dev.spark_db.sf_fire_calls
      where CallType is not null
      group by CallType, Zipcode
      order by count desc
     */
    println("\nQ5 Result:")
    val q5_df = fire_df.select("CallType", "ZipCode")
      .filter(col("CallType").isNotNull)
      .groupBy("CallType", "ZipCode")
      .count()
      .orderBy(desc("count"))
    q5_df.show()

    /*
    Q6. What San Francisco neighborhoods are in the zip codes 94102 and 94103
      select distinct Neighborhood, Zipcode
      from dev.spark_db.sf_fire_calls
      where Zipcode== 94102 or Zipcode == 94103
     */
    println("\nQ6 Result:")
    val q6_df = fire_df.select("Neighborhood", "ZipCode")
      .filter(col("ZipCode").isin(94102, 94103))
      .distinct()
    q6_df.show()


    /*
    Q7. What was the sum of all calls, average, min and max of the response times for calls?
      select sum(NumAlarms), avg(Delay), min(Delay), max(Delay)
      from dev.spark_db.sf_fire_calls
     */
    println("\nQ7 Result:")
    val q7_df = fire_df.select(
      sum("NumAlarms").alias("Total Calls"),
      avg("Delay").alias("Average Response Time"),
      min("Delay").alias("Minimum Response Time"),
      max("Delay").alias("Maximum Response Time")
    )
    q7_df.show()

    /*
    Q8. How many distinct years of data is in the CSV file?
      select distinct year(CallDate) as year_num
      from dev.spark_db.sf_fire_calls
      order by year_num
     */
    println("\nQ8 Result:")
    val q8_df = fire_df.select(year(col("CallDate")).alias("year_num"))
      .distinct()
      .orderBy("year_num")
    q8_df.show()


    /*
    Q9. What week of the year in 2018 had the most fire calls?
      select weekofyear(CallDate) as week_year, count(*) as count
      from dev.spark_db.sf_fire_calls
      where year(CallDate) == 2018
      group by week_year
      order by count desc
     */
    println("\nQ9 Result:")
    val q9_df = fire_df.select(weekofyear(col("CallDate")).alias("week_year"))
      .filter(year(col("CallDate")) === 2018)
      .groupBy("week_year").count()
      .orderBy(desc("count"))
    q9_df.show()

    /*
    Q10. What neighborhoods in San Francisco had the worst response time in 2018?
      select Neighborhood, Delay
      from dev.spark_db.sf_fire_calls
      where year(CallDate) == 2018
     */
    println("\nQ10 Result:")
    val q10_df = fire_df.select("Neighborhood", "Delay")
      .filter(year(col("CallDate")) === 2018)
      .orderBy(desc("Delay"))
    q10_df.show()

  }
}
