name := "RDFBenchmarkingProject"

version := "0.1"

scalaVersion := "2.11.12"
Compile/mainClass := Some("RDFBenchMain")


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.0"
libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.6"
libraryDependencies += "postgresql" % "postgresql" % "8.4-702.jdbc4"
