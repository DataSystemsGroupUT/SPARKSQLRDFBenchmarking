package ee.ut.cs.bigdata.watdiv.querying.csv

import ee.ut.cs.bigdata.watdiv.querying.queries.WPTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object WidePropertyTable {
  def main(args: Array[String]): Unit = {

    println("Starting WPT VHDFS!")
    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench ORC WPT")
      .getOrCreate()

    val ds = args(0) // value = {"100M", "500M, or "1B"}
    var partitionType = args(1) // value = {"Horizontal", "Subject", or "Predicate"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/WPT/VHDFS/CSV"

    //read tables from HDFS
    val wptDF = spark.read.format("parquet").load(s"$path/WidePropertyTable.csv").toDF()

    wptDF.createOrReplaceTempView("WPT")


    //create file to write the query run time results
    // val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/parquet/ST/$ds$partitionType.txt"), true)

    val queries = List(
      new WPTQueries c1_prost_csv,
      new WPTQueries c2_prost_csv,
      new WPTQueries c3_prost_csv,
      new WPTQueries f1_prost_csv,
      new WPTQueries f2_prost_csv,
      new WPTQueries f3_prost_csv,
      new WPTQueries f4_prost_csv,
      new WPTQueries f4_prost_csv,
      new WPTQueries l1_prost_csv,
      new WPTQueries l2_prost_csv,
      new WPTQueries l3_prost_csv,
      new WPTQueries l4_prost_csv,
      new WPTQueries l5_prost_csv,
      new WPTQueries s1_prost_csv,
      new WPTQueries s2_prost_csv,
      new WPTQueries s3_prost_csv,
      new WPTQueries s4_prost_csv,
      new WPTQueries s5_prost_csv,
      new WPTQueries s6_prost_csv,
      new WPTQueries s7_prost_csv
    )


    var count = 1
    for (query <- queries) {
      //run query and calculate the run time
      val startTime = System.nanoTime()
      val df_count = spark.sql(query).count()
      println(df_count)
      //      spark.sql(query).show(1000,false)
      //df.take(100).foreach(println)
      //      val endTime = System.nanoTime()
      //      val result = (endTime - startTime).toDouble / 1000000000
      //
      //      //write the result into the log file
      //      if (count != queries.size) {
      //        Console.withOut(fos) {
      //          print(result + ",")
      //        }
      //      } else {
      //        Console.withOut(fos) {
      //          println(result)
      //        }
      //      }
      //      count += 1
    }

    println("All Queries are Done - Parquet - WPT!")


  }

}
