package ee.ut.cs.bigdata.sp2bench.csv

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.STQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel._


object ST_ORIGINAL {
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


    spark.conf.set("spark.sql.crossJoin.enabled", "true")
        
    val ds=args(0)
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/CSV/ST"

    val RDFDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/SingleStmtTable.csv").toDF()
    println(RDFDF.rdd.getNumPartitions)

    RDFDF.createOrReplaceTempView("SingleStmtTable")
    //RDFDF.persist(MEMORY_AND_DISK)

    println("Tables loaded!")

    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/csv/ST/$ds.txt"),true)
    val queries = List(new STQueries q1,
		       new STQueries q2,
		       new STQueries q3,
		       new STQueries q4,		       
		       new STQueries q5,
		       new STQueries q6,
		       new STQueries q7,
		       new STQueries q8,
	               new STQueries q9,
		       new STQueries q10,
		       new STQueries q11)
     
    var count = 1
    for (query <- queries)
    { 
       val starttime=System.nanoTime()
       val df=spark.sql(query)
       df.take(100).foreach(println)
       val endtime=System.nanoTime()
       val result = (endtime-starttime).toDouble/1000000000

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






