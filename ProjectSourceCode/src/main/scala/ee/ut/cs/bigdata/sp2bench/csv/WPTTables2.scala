package ee.ut.cs.bigdata.sp2bench.csv

import java.io.{File, FileOutputStream}

import ee.ut.cs.bigdata.sp2bench.queries.WPTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel._

object WPTTables2 {
  def main(args: Array[String]): Unit = {
    println("Started.. WPTTables2 - v15")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println("Conf created")

    val spark = SparkSession
      .builder()
      //      .config(conf)
      //      .master("spark://172.17.77.48:7077")
      //      .master("yarn-client")
      .config("spark.sql.broadcastTimeout", "36000")
      .appName(s"WPT")
      .getOrCreate()

    //    spark.sparkContext.setLogLevel("error")

    println("Spark Session created ")

    import spark.implicits._
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val hdfsURL = "hdfs://172.17.77.48:9000"
    val path = s"$hdfsURL/user/hadoop/RDFBench/SP2B/$ds/WPT/CSV"

    //read tables from HDFS

    val RDFDFWPT = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"$path/WidePropertyTable.csv")
      .toDF()

    RDFDFWPT.createOrReplaceTempView("WPT")

    //create file to write the query run time results    
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/csv/WPT/$ds.txt"), true)

    val queries = List(
      new WPTQueries q1,
      new WPTQueries q2,
      new WPTQueries q3,
      //            new WPTQueries q4,
      new WPTQueries q5,
      new WPTQueries q6,
      new WPTQueries q8,
      new WPTQueries q10,
      new WPTQueries q11,
    )

    var count = 1
    for (query <- queries) {
      // Run query and calculate the run time
      val starttime = System.nanoTime()

      val df = spark.sql(query)
      //      df.take(10).foreach(println)
      //      df.take(100).foreach(println)
      println("-------> RECORDS", df.count())

      val endtime = System.nanoTime()
      var result = ((endtime - starttime).toDouble / 1000000000).toString

      println(s"Took $result sec")

      //write the result into the log file
      if (count != queries.size) {
        result += ","
      }

      Console.withOut(fos) {
        print(result)
      }

      count += 1
    }

    println("All Queries are Done - CSV - WPT!")
  }
}
