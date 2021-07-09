package ee.ut.cs.bigdata.watdiv.avro

import ee.ut.cs.bigdata.watdiv.queries.STQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, FileOutputStream}

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
    //spark.conf.set("spark.sql.crossJoin.enabled", "true")

    val ds = args(0) // value = {"100M", "500M, or "1B"}
    var partitionType = args(1) // value = {"Horizontal", "Subject", or "Predicate"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/ST/Avro"

    //read tables from HDFS
    val RDFDF = spark.read.format("avro").load(s"$path/ST$ds.avro").toDF()

    RDFDF.createOrReplaceTempView("Triples")

    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/csv/ST/$ds$partitionType.txt"), true)

    val queries = List(
      new STQueries q1,
      new STQueries q2,
      new STQueries q3,
      new STQueries q4,
      new STQueries q5,
      new STQueries q6,
      new STQueries q7,
      new STQueries q8,
      new STQueries q9,
      new STQueries q10,
      new STQueries q11,
      new STQueries q12,
      new STQueries q13,
      new STQueries q14,
      new STQueries q15,
      new STQueries q16,
      new STQueries q17,
      new STQueries q18,
      new STQueries q19,
      new STQueries q20)

    var count = 1
    for (query <- queries) {
      //run query and calculate the run time
      val startTime = System.nanoTime()
      val df = spark.sql(query)
      df.take(100).foreach(println)
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

    println("All Queries are Done - CSV - ST!")

  }
} 
