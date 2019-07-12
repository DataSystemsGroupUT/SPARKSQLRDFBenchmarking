package ee.ut.cs.bigdata.sp2bench.parquet

import java.io.{File, FileOutputStream}

import ee.ut.cs.bigdata.sp2bench.queries.STQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SingleStatementTable {


  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local").setAppName("SQLSPARK")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")


    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SparkSessionZipsExample")
      .getOrCreate()

    val ds="10M"
    val path=s"hdfs://quickstart:8020/user/cloudera/RDFBench/SP2B/$ds/Parquet"

    val RDFDF = spark.read.format("parquet").load(s"$path/SingleStmtTable").toDF()
    RDFDF.createOrReplaceTempView("SingleStmtTable")


    val fos = new FileOutputStream(new File(s"/home/cloudera/Downloads/Results/parquet/ST/$ds.txt"),true)

    Console.withOut(fos) {spark.time(spark.sql(new STQueries q1).show())}
    Console.withOut(fos) {spark.time(spark.sql(new STQueries q2).show())}
    Console.withOut(fos) {spark.time(spark.sql(new STQueries q3).show())}
    Console.withOut(fos) {spark.time(spark.sql(new STQueries q4).show())}
    Console.withOut(fos) {spark.time(spark.sql(new STQueries q5).show())}
    Console.withOut(fos) {spark.time(spark.sql(new STQueries q6).show())}
    Console.withOut(fos) {spark.time(spark.sql(new STQueries q8).show())}
    Console.withOut(fos) {spark.time(spark.sql(new STQueries q9).show())}
    Console.withOut(fos) {spark.time(spark.sql(new STQueries q10).show())}
    Console.withOut(fos) {spark.time(spark.sql(new STQueries q11).show())}

    Console.withOut(fos) {spark.time(spark.sql(new STQueries q7).show())}


    Console.withOut(fos) {println("===================================")}
    println("All Queries are Done!")

  }

}
