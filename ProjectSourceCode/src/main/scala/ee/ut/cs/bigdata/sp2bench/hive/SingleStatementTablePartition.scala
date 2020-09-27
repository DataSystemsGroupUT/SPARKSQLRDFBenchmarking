package ee.ut.cs.bigdata.sp2bench.hive

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.STQueries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SingleStatementTablePartition {
  def main(args: Array[String]): Unit = {

   val warehouseLocation = "hdfs://172.17.77.48:9000/user/hive/warehouse"
   Logger.getLogger("org").setLevel(Level.OFF)
   Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("HIVE ST")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris","thrift://172.17.77.48:9083")
      .enableHiveSupport()
      .getOrCreate()

   import spark.implicits._
   val db = "rdfbench"
   val ds = args(0)				//value = {"100M", "500M, or "1B"}
   var partitionType=args(1).toLowerCase	//value = {"Horizontal", "Subject", or "Predicate"}

   //use Hive database to read non-partitioned tables
   var hiveDB= db.concat(ds)
   spark.sql(s"USE $hiveDB")

   //read table from Hive
   val RDFDF = spark.sql("SELECT * FROM singlestmttable").toDF()

   //partition and save into Hive
   if(partitionType == "subject")
   {
     hiveDB = hiveDB+partitionType
     spark.sql(s"USE $hiveDB")
     RDFDF.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".singlestmttable")
     println("Hive ST partitioned and saved! Subject based partitioning!")
   }

   else if (partitionType == "predicate")
   {
     hiveDB = hiveDB+partitionType
     spark.sql(s"USE $hiveDB")
     RDFDF.repartition(84, $"Predicate").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".singlestmttable")
     println("Hive ST partitioned and saved! Predicate based partitioning!") 
   }

   else if (partitionType == "horizontal")
   {
     hiveDB = hiveDB+partitionType
     spark.sql(s"USE $hiveDB")
     RDFDF.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".singlestmttable")
     println("Hive ST partitioned and saved! Horizontal partitioning!") 
   }         

  }
}
