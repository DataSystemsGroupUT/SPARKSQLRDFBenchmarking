package ee.ut.cs.bigdata.watdiv.csv

import ee.ut.cs.bigdata.watdiv.queries.PTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, FileOutputStream}

object PropertyTables {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench CSV PT")
      .getOrCreate()
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/PT/CSV"

    //read tables from HDFS

    // PUT DFs of PT Here

    //create file to write the query run time results    
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/csv/PT/$ds.txt"), true)

    val queries = List(new PTQueries q1,
      new PTQueries q2,
      new PTQueries q3,
      new PTQueries q4,
      new PTQueries q5,
      new PTQueries q6,
      new PTQueries q8,
      new PTQueries q10,
      new PTQueries q11,
      new PTQueries q12,
      new PTQueries q13,
      new PTQueries q14,
      new PTQueries q15,
      new PTQueries q16,
      new PTQueries q17,
      new PTQueries q18,
      new PTQueries q19,
      new PTQueries q20)

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
    println("All Queries are Done - CSV - PT!")

  }
}
