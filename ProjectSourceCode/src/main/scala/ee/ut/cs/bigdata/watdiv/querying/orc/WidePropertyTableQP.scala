package ee.ut.cs.bigdata.watdiv.querying.orc

import ee.ut.cs.bigdata.watdiv.querying.queries.WPTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, FileOutputStream}

object WidePropertyTableQP {
  def main(args: Array[String]): Unit = {

    println("Starting Querying WPT Partitioned ORC!")

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench ORC WPT Partitioned")
      .getOrCreate()
    //spark.conf.set("spark.sql.crossJoin.enabled", "true")

    val ds = args(0) // value = {"100M", "500M, or "1B"}
    var partitionType = args(1) // value = {"Horizontal", "Subject", or "Predicate"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/WPT"

    val WPT_DF = spark.read.format("orc").load(s"$path/$partitionType/ORC/WPT$ds.orc").toDF()

    WPT_DF.createOrReplaceTempView("WPT")

    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/orc/WPT/$partitionType.txt"), true)

    val queries = List(
      new WPTQueries c1_prost,
      //new WPTQueries c2_prost,
      new WPTQueries c3_prost,
      new WPTQueries f1_prost,
      new WPTQueries f2_prost,
      new WPTQueries f3_prost,
      new WPTQueries f4_prost,
      new WPTQueries f5_prost,
      new WPTQueries l1_prost,
      new WPTQueries l2_prost,
      new WPTQueries l3_prost,
      new WPTQueries l4_prost,
      new WPTQueries l5_prost,
      new WPTQueries s1_prost,
      new WPTQueries s2_prost,
      new WPTQueries s3_prost,
      new WPTQueries s4_prost,
      new WPTQueries s5_prost,
      new WPTQueries s6_prost,
      new WPTQueries s7_prost
    )

    var count = 1
    for (query <- queries) {
      //run query and calculate the run time
      val startTime = System.nanoTime()
      val df_count = spark.sql(query).count()
      println(df_count)
      //df.take(100).foreach(println)
      val endTime = System.nanoTime()
      val result = (endTime - startTime).toDouble / 1000000000

      //write the result into the log file
      if (count != queries.size) {
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

    println("All Queries are Done - ORC - WPT!")

  }
} 
