package ee.ut.cs.bigdata.watdiv.querying.orc

import ee.ut.cs.bigdata.watdiv.querying.queries.VTQueries
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, FileOutputStream}

object VerticalTablesQP {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench ORC VT")
      .getOrCreate()
    val ds = args(0) // value = {"100M", "500M, or "1B"}
    var partitionType = args(1) // value = {"Horizontal", "Subject", or "Predicate"}

    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/VP/"


    println("VP Querying!")
    FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(s"$path/$partitionType/ORC")).foreach {
      x =>
        val vpTable = spark.read.format("orc").load(x.getPath().toString)
        vpTable.createOrReplaceTempView(x.getPath().getName().substring(0, x.getPath().getName().lastIndexOf('.')))
    }


    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/orc/VP/$partitionType.txt"), true)

    val queries = List(
      new VTQueries c1
//      ,
//      new VTQueries c2,
//      new VTQueries c3,
//      new VTQueries f1,
//      new VTQueries f2,
//      new VTQueries f3,
//      new VTQueries f4,
//      new VTQueries f5,
//      new VTQueries l1,
//      new VTQueries l2,
//      new VTQueries l3,
//      new VTQueries l4,
//      new VTQueries l5,
//      new VTQueries s1,
//      new VTQueries s2,
//      new VTQueries s3,
//        new VTQueries s4
//      ,
//      new VTQueries s5,
//      new VTQueries s6,
//      new VTQueries s7
          )

    var count = 1
    for (query <- queries) {
      //run query and calculate the run time
      val startTime = System.nanoTime()
//      val df_count = spark.sql(query).count()
//      println(df_count)

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


    println("All Queries are Done - ORC - VP!")

  }
}
