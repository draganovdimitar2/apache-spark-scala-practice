package spark.practice

import org.apache.spark.sql.functions.{
  col,
  expr,
  lit,
  monotonically_increasing_id,
  to_date
}

object Joins {

  case class Person(name: String, birthdate: String, salary: Double)

  case class Department(personId: Option[Int], department: Option[String])

  def main(args: Array[String]): Unit = {
    val spark = Utils.getSparkSession
    import spark.implicits._

    val person_df_raw = Seq(
      Person("Ivan", "15032001", 2500.50),
      Person("Ceca", "02032000", 5555.55),
      Person("Dragan", "02031999", 3321.55)
    ).toDF()
      .withColumn("id", monotonically_increasing_id())
      .select("id", "name", "birthdate", "salary")

    // Convert date to date type
    val person_df_final = person_df_raw.withColumn(
      "birthdate",
      to_date(col("birthdate"), "ddMMyyyy")
    )

    person_df_final.show()

    val department_df = Seq(
      Department(Some(0), Some("Marketing")),
      Department(Some(1), Some("IT")),
      Department(None, Some("Finance")),
      Department(None, None)
    ).toDF()

    department_df.show()

    // innerJoin
    val inner_joined_Df = person_df_final.join(
      department_df,
      person_df_final("id") === department_df("personId"),
      "inner"
    )

    println("Inner join:")
    inner_joined_Df.show()

    // leftJoin
    val left_joined_df = person_df_final.join(
      department_df,
      person_df_final("id") === department_df("personId"),
      "left"
    )

    println("Left join:")
    left_joined_df.show()

    // rightJoin
    val right_joined_df = person_df_final.join(
      department_df,
      person_df_final("id") === department_df("personId"),
      "right"
    )

    println("Right join:")
    right_joined_df.show()

    // outerJoin
    val outer_joined_df = person_df_final.join(
      department_df,
      person_df_final("id") === department_df("personId"),
      "outer"
    )

    println("Outer join:")
    outer_joined_df.show()
  }
}
