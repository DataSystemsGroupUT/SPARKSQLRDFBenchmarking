package ee.ut.cs.bigdata.sp2bench.csv

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.WPTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel._

object Tests {
  def main(args: Array[String]): Unit = {
    println("Running new thing 3")

    val spark = SparkSession
      .builder()
      //      .config(conf)
      //      .master("spark://172.17.77.48:7077")
      //      .master("yarn-client")
      .config("spark.sql.broadcastTimeout", "36000")
      .appName(s"WPT")
      .master("spark://172.17.77.48:7077")
      .getOrCreate()

    val hdfsURL = "hdfs://172.17.77.48:9000"
    val path = s"$hdfsURL/user/hadoop/RDFBench/SP2B/250M/WPT/CSV"

    val RDFDFWPT = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"$path/WidePropertyTable.csv")
      .toDF()

    println("Got it")

    spark.stop()
    /*
    val conf = new SparkConf(true)

    //    Logger.getLogger("org").setLevel(Level.OFF)
    //    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName(s"Testing")
      .getOrCreate()

    val hdfsURL = "hdfs://172.17.77.48:9000"

    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/out.txt"), true)
    //    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/250M/csv/WPT/250.txt"), true)
    //          .csv(s"$hdfsURL/user/hadoop/RDFBench/SP2B/250M/WPT/CSV/WidePropertyTable.csv")

    val df = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(s"$hdfsURL/user/hadoop/RDFBench/SP2B/100K/CSV/WPT/WPTAbstractPredicate.csv")
      .toDF()

    println("Result: ", df.count())
*/
    //
    //    df.createOrReplaceTempView("WPT")
    //
    //    println("RESULT!!!", df.count())
    //
    //    val query = new WPTQueries q4;
    //
    //    val startTime = System.nanoTime
    //    val res = spark.sql(query)
    //
    //    res.take(50).foreach(println)
    //
    //    val elapsedSec = System.nanoTime - startTime / 1e9d
    //
    //    println("FINISHED IN", elapsedSec)
    //    Console.withOut(fos) {
    //      print(elapsedSec)
    //    }
  }
}