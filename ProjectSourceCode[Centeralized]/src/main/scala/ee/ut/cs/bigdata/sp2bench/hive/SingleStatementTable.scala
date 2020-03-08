package ee.ut.cs.bigdata.sp2bench.hive

import java.io.{File, FileOutputStream}

import ee.ut.cs.bigdata.sp2bench.queries.STQueries
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level


object SingleStatementTable {
  def main(args: Array[String]): Unit = {


  val warehouseLocation = "/user/hive/warehouse"

   Logger.getLogger("org").setLevel(Level.OFF)
   Logger.getLogger("akka").setLevel(Level.OFF)

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


   val fos = new FileOutputStream(new File(s"/home/cloudera/Downloads/Results/hive/ST/$hiveDB.txt"),true)

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
