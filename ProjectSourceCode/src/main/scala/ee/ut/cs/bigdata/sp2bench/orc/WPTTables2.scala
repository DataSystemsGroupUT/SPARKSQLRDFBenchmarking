package ee.ut.cs.bigdata.sp2bench.orc

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.WPTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel._


object WPTTables2 {
  def main(args: Array[String]): Unit = {
    println("Starting ORC")

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench ORC WPT")
      .getOrCreate()
    println("Spark Session created!")

    import spark.implicits._
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val hdfsURL = "hdfs://172.17.77.48:9000"
    val path = s"$hdfsURL/user/hadoop/RDFBench/SP2B/$ds/WPT/ORC"

    //read tables from HDFS
    val RDFDFWPT = spark.read.format("orc").load(s"$path/WidePropertyTable.orc").toDF()
    RDFDFWPT.createOrReplaceTempView("WPT")


    //create file to write the query run time results    
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/orc/WPT/$ds"+"VHDFS.txt"), true)

    val queries = List(
      new WPTQueries q1,
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
    println("All Queries are Done - ORC - WPT!")

  }
}
