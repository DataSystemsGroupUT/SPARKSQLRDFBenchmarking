package ee.ut.cs.bigdata.sp2bench.hive

import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException
import ee.ut.cs.bigdata.sp2bench.queries.WPTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WPTTables2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val warehouseLocation = "hdfs://172.17.77.48:9000/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName("rdfbench Hive WPT")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris", "thrift://172.17.77.48:9083")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val db = "rdfbench"
    val ds = args(0) //value = {"100M", "500M, or "1B"}

    //use partitioned db
    var hiveDB = db.concat(ds)
    spark.sql(s"USE $hiveDB")

    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/hive/WPT/$ds.txt"), true)

    val queries = List(
      new WPTQueries q1,
      new WPTQueries q2,
      new WPTQueries q3,
      new WPTQueries q4,
      new WPTQueries q5,
      new WPTQueries q6,
      new WPTQueries q8,
      new WPTQueries q10,
      new WPTQueries q11,
    )

    var count = 1
    for (query <- queries) {
      val starttime = System.nanoTime()
      val df = spark.sql(query)
      println("-------> RECORDS", df.count())
      //      df.take(100).foreach(println)
      val endtime = System.nanoTime()
      val result = (endtime - starttime).toDouble / 1000000000

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
    println("All Queries are Done - HIVE - WPT!")
  }
}
