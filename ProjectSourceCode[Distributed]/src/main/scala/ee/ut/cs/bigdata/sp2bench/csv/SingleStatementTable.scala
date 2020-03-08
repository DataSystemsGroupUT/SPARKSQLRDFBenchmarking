package ee.ut.cs.bigdata.sp2bench.csv

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.STQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel._

object SingleStatementTable {
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
        
    val ds=args(0)		 		// value = {"100M", "500M, or "1B"} 
    var partitionType = args(1)  		// value = {"Horizontal", "Subject", or "Predicate"}
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/CSV/ST"
                     
    //read tables from HDFS
    val RDFDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/SingleStmtTable$partitionType.csv").toDF()
    RDFDF.createOrReplaceTempView("SingleStmtTable")

    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/csv/ST/$ds$partitionType.txt"),true)

    val queries = List(new STQueries q1,
		       new STQueries q2,
		       new STQueries q3,
		       new STQueries q4,
		       new STQueries q5,
		     //  new STQueries q6,
		     //  new STQueries q7)
		       new STQueries q8,
	               new STQueries q9,
		       new STQueries q10,
		       new STQueries q11)            

    var count = 1
    for (query <- queries)
    { 
       //run query and calculate the run time
       val starttime=System.nanoTime()
       val df=spark.sql(query)
       df.take(100).foreach(println)
       val endtime=System.nanoTime()
       val result = (endtime-starttime).toDouble/1000000000

       //write the result into the log file
       if( count != queries.size ) {
           Console.withOut(fos){print(result + ",")}
       } else {
           Console.withOut(fos){println(result)}
       }
       count+=1   
    }              
    println("All Queries are Done - CSV - ST!") 

  }
} 
