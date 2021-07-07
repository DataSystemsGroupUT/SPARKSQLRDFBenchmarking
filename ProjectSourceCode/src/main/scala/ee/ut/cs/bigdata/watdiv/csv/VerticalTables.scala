package ee.ut.cs.bigdata.watdiv.csv

import ee.ut.cs.bigdata.watdiv.queries.VTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, FileOutputStream}

object VerticalTables {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench CSV VT")
      .getOrCreate()
    val ds = args(0) // value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds"

    //read tables from HDFS

    // VP DFS and Tables HERE

    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/csv/VP/$ds.txt"), true)

    val queries = List(new VTQueries q1,
      new VTQueries q2,
      new VTQueries q3,
      new VTQueries q4,
      new VTQueries q5,
      new VTQueries q6,
      new VTQueries q7,
      new VTQueries q8,
      new VTQueries q9,
      new VTQueries q10,
      new VTQueries q11,
      new VTQueries q12,
      new VTQueries q13,
      new VTQueries q14,
      new VTQueries q15,
      new VTQueries q16,
      new VTQueries q17,
      new VTQueries q18,
      new VTQueries q19,
      new VTQueries q20)

    var count = 1
    for (query <- queries) {
      //run query and calculate the run time
      val starttime = System.nanoTime()
      val df = spark.sql(query)
      df.take(100).foreach(println)
      val endtime = System.nanoTime()
      val result = (endtime - starttime).toDouble / 1000000000

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
    println("All Queries are Done - CSV - VP!")

  }
}
