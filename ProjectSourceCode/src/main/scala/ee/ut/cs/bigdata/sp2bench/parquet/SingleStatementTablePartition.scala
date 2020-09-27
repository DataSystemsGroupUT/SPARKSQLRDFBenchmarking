package ee.ut.cs.bigdata.sp2bench.parquet

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.STQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SingleStatementTablePartition {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Parquet ST")
      .getOrCreate()

    import spark.implicits._
	      
    val ds=args(0)				//value = {"100M", "500M, or "1B"}
    var partitionType=args(1).toLowerCase	//value = {"Horizontal", "Subject", or "Predicate"}
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/Parquet/ST"

    //read table from HDFS
    val RDFDF = spark.read.format("parquet").load(s"$path/SingleStmtTable").toDF()
    println("Original table loaded!")

    //partition and save on HDFS
    if(partitionType == "subject")
    {
     RDFDF.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/SingleStmtTableSubject") 
     println("Parquet ST partitioned and saved! Subject based Partitioning!")
    }

    else if (partitionType == "predicate")
    {
      RDFDF.repartition(84, $"Predicate").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/SingleStmtTablePredicate")
      println("Parquet ST partitioned and saved! Predicate based Partitioning!")
    }

    else if (partitionType == "horizontal")
    {
      RDFDF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/SingleStmtTableHorizontal")
      println("Parquet ST partitioned and saved! Horizontal partitioning!")
    }
         
  }
}
