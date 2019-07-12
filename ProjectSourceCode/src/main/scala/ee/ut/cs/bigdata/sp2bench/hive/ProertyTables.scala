package ee.ut.cs.bigdata.sp2bench.hive


import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException

import ee.ut.cs.bigdata.sp2bench.queries.PTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ProertyTables {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val warehouseLocation = "/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SparkSessionZipsExample")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris","thrift://quickstart.cloudera:9083")
      .enableHiveSupport()
      .getOrCreate()




    val hiveDB="rdfbench10m"
    spark.sql(s"USE $hiveDB")



    val fos = new FileOutputStream(new File(s"/home/cloudera/Downloads/Results/hive/PT/$hiveDB.txt"),true)



    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q1).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q2).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q3).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q4).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q5).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q6).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q8).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q10).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q11).show) }

    try{Console.withOut(fos) {spark.time(spark.sql(new PTQueries q7).show())}}
    catch {case toe: TimeoutException=>Console.withOut(fos) {println("Time taken:  ms")}}

    Console.withOut(fos) {println("===================================")}
    println("All Queries are Done!")


  }

}
