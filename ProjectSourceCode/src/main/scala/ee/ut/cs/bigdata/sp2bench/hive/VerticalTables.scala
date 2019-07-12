package ee.ut.cs.bigdata.sp2bench.hive




  import java.io.{File, FileOutputStream}

  import ee.ut.cs.bigdata.sp2bench.queries.VTQueries
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.SparkSession

object VerticalTables {

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


    val fos = new FileOutputStream(new File(s"/home/cloudera/Downloads/Results/hive/VT/$hiveDB.txt"),true)


    Console.withOut(fos) {"Q1:"+spark.time(spark.sql(new VTQueries q1).show)}
    Console.withOut(fos) {"Q2:"+spark.time(spark.sql(new VTQueries q2).show)}
    Console.withOut(fos) {"Q3:"+spark.time(spark.sql(new VTQueries q3).show)}
    Console.withOut(fos) {"Q4:"+spark.time(spark.sql(new VTQueries q4).show)}
    Console.withOut(fos) {"Q5:"+spark.time(spark.sql(new VTQueries q5).show)}
    Console.withOut(fos) {"Q6:"+spark.time(spark.sql(new VTQueries q6).show)}
    Console.withOut(fos) {"Q8:"+spark.time(spark.sql(new VTQueries q8).show)}
    Console.withOut(fos) {"Q9:"+spark.time(spark.sql(new VTQueries q9).show)}
    Console.withOut(fos) {"Q10:"+spark.time(spark.sql(new VTQueries q10).show)}
    Console.withOut(fos) {"Q11:"+spark.time(spark.sql(new VTQueries q11).show)}

    Console.withOut(fos) {"Q7:"+spark.time(spark.sql(new VTQueries q7).show())}

    Console.withOut(fos) {println("=======================================")}
    println("All Queries are Done!")




  }

}
