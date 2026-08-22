package spark.practice

import com.databricks.connect.DatabricksSession
import org.apache.spark.sql.connect.SparkSession


object Utils {
  def getSparkSession: SparkSession = {
    val host = sys.env.getOrElse("DATABRICKS_HOST",
      throw new RuntimeException("Environment var DATABRICKS_HOST is not set."))
    val token = sys.env.getOrElse("DATABRICKS_TOKEN",
      throw new RuntimeException("Environment var DATABRICKS_TOKEN is not set."))
    return DatabricksSession.builder()
      .host(host)
      .token(token)
      .serverless()
      .getOrCreate()

  }
}
