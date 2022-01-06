package ee.ut.cs.bigdata.watdiv.querying.orc

import ee.ut.cs.bigdata.watdiv.querying.queries.ExtVPQueries
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, FileOutputStream}

object ExtVerticalTablesQP {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench ORC ExtVP")
      .getOrCreate()
    val ds = args(0) // value = {"100M", "500M, or "1B"}
    var partitionType = args(1) // value = {"Horizontal", "Subject", or "Predicate"}

    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/ExtVP/"


    println("ExtVP Querying!")
    FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(s"$path/$partitionType/ORC")).foreach {
      x =>
        val extVPTable = spark.read.format("orc").load(x.getPath().toString)
        extVPTable.createOrReplaceTempView(x.getPath().getName().substring(0, x.getPath().getName().lastIndexOf('.')))
    }


    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/watdiv/$ds/orc/ExtVP/$partitionType$ds.txt"), true)

    val queries = List(
      new ExtVPQueries C1,
      new ExtVPQueries C2,
      new ExtVPQueries C3,
      new ExtVPQueries F1,
      new ExtVPQueries F2,
      new ExtVPQueries F3,
      new ExtVPQueries F4,
      new ExtVPQueries F5,
      new ExtVPQueries L1,
      new ExtVPQueries L2,
      new ExtVPQueries L3,
      new ExtVPQueries L4,
      new ExtVPQueries L5,
      new ExtVPQueries S1,
      new ExtVPQueries S2,
      new ExtVPQueries S3,
      new ExtVPQueries S4,
      new ExtVPQueries S5,
      new ExtVPQueries S6,
      new ExtVPQueries S7)

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


    println("All Queries are Done - ORC - ExtVP!")

  }
}
