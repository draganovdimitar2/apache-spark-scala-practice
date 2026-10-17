package spark.practice
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{
  array,
  col,
  explode,
  lit,
  struct,
  to_json
}

case class City(id: Long, source: String, destination: String, distance: Long)
object FilteringAndRemovingDuplicates {
  def main(args: Array[String]): Unit = {
    val spark = Utils.getSparkSession
    import spark.implicits._

    /*
        Create a test Dataframe as shown below
        +---+---------+-----------+--------+
        | id|   source|destination|distance|
        +---+---------+-----------+--------+
        |101|   Mumbai|        Goa|     587|
        |102|   Mumbai|  Bangalore|     985|
        |102|   Mumbai|  Bangalore|     985|
        |103|    Delhi|    Chennai|    2208|
        |104|    Delhi|    Chennai|    2208|
        |105|Bangalore|    Kolkata|    1868|
        |105|Bangalore|    Kolkata|    1865|
        +---+---------+-----------+--------+
     */
    val df = Seq(
      City(101, "Mumbai", "Goa", 587),
      City(102, "Mumbai", "Bangalore", 985),
      City(102, "Mumbai", "Bangalore", 985),
      City(103, "Delhi", "Chennai", 2208),
      City(104, "Delhi", "Chennai", 2208),
      City(105, "Bangalore", "Kolkata", 1868),
      City(105, "Bangalore", "Kolkata", 1865)
    ).toDF()
    // df.show()

    // df.printSchema()

    /*
      Select records that satisfy the following criteria
        1. Source is Mumbai and destination is Bangalore
     */
    // Using a single filter
    // df.filter("source == 'Mumbai' and destination == 'Bangalore'").show()

    // Using two filters
    // df.filter("source == 'Mumbai'").filter("destination == 'Bangalore'").show()

    // Using a col/$ function expression
    // df.filter($"source" === "Mumbai" && $"destination" === "Bangalore").show()

    // Using dataframe qualifier
    // df.filter(df("source") === "Mumbai" && df("destination") === "Bangalore").show()

    // Removing Duplicates
    // df.distinct().show()

    // Remove duplicates based on the specified column values
    df.dropDuplicates(Seq("id", "source", "destination")).show()

  }
}
