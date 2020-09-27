name := "RDFBenchmarkingProject"

version := "0.1"

scalaVersion := "2.12.8"
mainClass in (Compile, run) := Some("ee.ut.cs.bigdata.sp2bench.RDFBenchMain")
//Compile/mainClass := Some("RDFBenchMain")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.2"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.2"
libraryDependencies += "org.apache.spark" %% "spark-avro"  % "2.4.2"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.5"


//libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.6"
//libraryDependencies += "postgresql" % "postgresql" % "8.4-702.jdbc4"

