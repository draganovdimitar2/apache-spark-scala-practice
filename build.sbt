name := "apache-spark-scala-practice"
version := "0.1"
scalaVersion := "2.13.16"

val sparkVersion = "4.0.1"
val connectVersion = "17.0.+"

libraryDependencies ++= Seq(
  "com.databricks" %% "databricks-connect" % connectVersion
)