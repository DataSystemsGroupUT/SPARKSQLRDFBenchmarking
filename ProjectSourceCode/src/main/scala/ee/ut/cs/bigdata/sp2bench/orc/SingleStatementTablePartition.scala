package ee.ut.cs.bigdata.sp2bench.orc

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.STQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import java.util.concurrent.TimeoutException

object SingleStatementTablePartition {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench ORC ST")
      .getOrCreate()

     import spark.implicits._
	      
     val ds=args(0)						// data size
     var partitionType=args(1).toLowerCase  	// horizontal, predicate or subject    
     val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/ORC/ST" 

     //read table from HDFS
     val RDFDF = spark.read.format("orc").load(s"$path/SingleStmtTable").toDF()

     //partition and save on HDFS
     if(partitionType == "subject")
     {
       RDFDF.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/SingleStmtTableSubject")
       println("ORC ST partitioned and saved! Subject based partitioning")
     }

     else if (partitionType == "predicate")
     {
       RDFDF.repartition(84, $"Predicate").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/SingleStmtTablePredicate")
       println("ORC ST partitioned and saved! Predicate based partitioning")
     }

     else if (partitionType == "horizontal")
     {
       RDFDF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/SingleStmtTableHorizontal")
       println("ORC ST partitioned and saved! Horizontal partitioning")
     }         

  }
}
