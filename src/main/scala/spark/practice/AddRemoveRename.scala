package spark.practice
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.Row
import scala.jdk.CollectionConverters._
import org.apache.spark.sql.functions.{col, expr, lit}

object AddRemoveRename {
  def main(args: Array[String]): Unit = {
    val spark = Utils.getSparkSession

        /* Create a dataframe as below
      +---+--------+---+------+
      | id|    name|age|salary|
      +---+--------+---+------+
      |100|Prashant| 45| 45000|
      |101|   Tarun| 36| 33000|
      |102|   David| 48| 28000|
      +---+--------+---+------+
     */

    val schema = DataType.fromDDL("id INT, name STRING, age INT, salary DOUBLE").asInstanceOf[StructType]

    val data = Seq(
      Row(100, "Prashant", 45, 45000.0),
      Row(101, "Ivan", 40, 50000.0),
      Row(102, "Petar", 22, 30000.0)
    ).asJava

    val sample_df = spark.createDataFrame(data, schema)

    /*
      Add following columns to your Dataframe
        1. increment: 10% of the salary up to 3000 maximum increment
        2. revised_salary: salary + increment
     */

    val q1_df = sample_df.withColumns(Map(
      "increment" -> expr("case when salary > 30000 then 3000 else salary * 10/100 end"),
      "revised_salary" -> expr("salary + increment")
    ))
    //q1_df.show()


    /*
      Add following columns to your Dataframe
        1. increment: 10% of the salary up to 3000 maximum increment
      Replace the following column in your dataframe
        2. salary: current salary + increment
     */

    val q2_df = sample_df.withColumn("increment", expr("case when salary > 30000 then 3000 else 10/100 * salary end"))
      .withColumn("salary", expr("salary + increment"))

    //q2_df.show()

    //Add a batch number (uuid) column to your dataframe

    val q3_df = sample_df.withColumn("batch_id", expr("uuid()"))
    // q3_df.show(truncate=false)
    // q3_df.printSchema()
    
    /*
      Rename the dataframe columns as listed below (Use q2_df)
        1. increment - annual_increment
        2. salary - incremented_salary
    */

    val q4_df = q2_df.withColumnsRenamed(Map(
      "increment" -> "annual_increment",
      "salary" -> "incremented_salary"
    ))

    // q4_df.show()

        
    /*
      Remove the following columns from your dataframe (Use q4_df)
        1. age
        2. annual_increment
     */

    val q5_df = q4_df.drop("age", "annual_increment")
    q5_df.show() 
  }
}
