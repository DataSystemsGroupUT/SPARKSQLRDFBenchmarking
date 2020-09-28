package ee.ut.cs.bigdata.sp2bench.csv

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.WPTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel._

object WPTTables2 {
  def main(args: Array[String]): Unit = {
    println("Started.. WPTTables2")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // 1) Create Morpheus session
    val conf = new SparkConf(true)
    //conf.set("spark.driver.memory","100g")
    conf.set("spark.executor.memory", "100g")
    conf.set("spark.sql.codegen.wholeStage", "true")
    conf.set("spark.sql.shuffle.partitions", "12")
    conf.set("spark.master", "yarn-client")
    conf.set("spark.default.parallelism", "8")

    println("Conf created")

    val spark = SparkSession
      .builder()
      .config(conf)
            .master("spark://172.17.77.48:7077")
      //      .master("yarn-client")
      .appName(s"WPT")
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")

    println("Spark Session created ")

    import spark.implicits._
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/WPT/CSV"


    //read tables from HDFS

    val RDFDFWPT = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTable.csv").toDF()
    RDFDFWPT.createOrReplaceTempView("WPT")

    //create file to write the query run time results    
    //    val fos = new FileOutputStream(new File(s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/logs_NP_250M/250M/csv/WPT/250M/$ds.txt"), true)
    //    val fos = new FileOutputStream(new File(s"/user/hadoop/RDFBench/logs_NP_250M/250M/csv/WPT/250M/$ds.txt"), true)
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/250M/csv/WPT/250.txt"), true)

    println("1")

    val queries = List(new WPTQueries q1,
      new WPTQueries q2,
      new WPTQueries q3,
      new WPTQueries q4,
      new WPTQueries q5,
      new WPTQueries q6,
      new WPTQueries q8,
      new WPTQueries q10,
      new WPTQueries q11)

    var count = 1
    for (query <- queries) {
      //run query and calculate the run time
      val starttime = System.nanoTime()

      // Timeout

      val df = spark.sql(query)
      df.take(100).foreach(println)
      val endtime = System.nanoTime()
      val result = (endtime - starttime).toDouble / 1000000000

      //write the result into the log file
      if (count != queries.size) {
        println("/////////" + result)
        Console.withOut(fos) {
          print(result + ",")
        }
      } else {
        Console.withOut(fos) {
          println(result)
        }
      }
      count += 1
    }
    println("All Queries are Done - CSV - WPT!")

  }
}
