package ee.ut.cs.bigdata.sp2bench.csv

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.STQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel._

object SingleStatementTablePartition {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench CSV ST")
      .getOrCreate()

    import spark.implicits._
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
        
    val ds=args(0)						// data size
    var partitionType=args(1).toLowerCase	// horizontal, predicate or subject
   
   val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/CSV/ST"
   
   //read original table
   val RDFDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/SingleStmtTable.csv").toDF()


   //partition and save on HDFS
   if(partitionType == "subject")
   {
     RDFDF.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/SingleStmtTableSubject.csv")
     println("CSV ST partitioned and saved! Subject!")
   }

   else if (partitionType == "predicate")
   {
     RDFDF.repartition(84, $"Predicate").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/SingleStmtTablePredicate.csv")
     println("CSV ST partitioned and saved! Predicate!")
   }

   else if (partitionType == "horizontal")
   {
     RDFDF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/SingleStmtTableHorizontal.csv")
     println("CSV ST partitioned and saved! Horizontal!")
   }
         

  }
} 

