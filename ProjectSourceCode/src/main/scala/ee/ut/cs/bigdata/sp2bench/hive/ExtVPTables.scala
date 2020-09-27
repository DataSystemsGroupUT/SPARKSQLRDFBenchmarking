package ee.ut.cs.bigdata.sp2bench.hive

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.ExtVPQueries
import ee.ut.cs.bigdata.sp2bench.queries.VTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ExtVPTables {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val warehouseLocation = "hdfs://172.17.77.48:9000/user/hive/warehouse"
    val spark = SparkSession
      .builder()
      .appName("rdfbench Hive ExtVP")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris","thrift://172.17.77.48:9083")
      .enableHiveSupport()
      .getOrCreate()
   
    import spark.implicits._    

    val db = "rdfbench"
    val ds = args(0)		//value = {"100M", "500M, or "1B"} 
    var partitionType=args(1)   //value = {"Horizontal", "Subject", or "Predicate"}

    //use partitioned db
    var hiveDB= db.concat(ds)

    //use partitioned ExtVP tables if it is 'subject' or 'horizontal', otherwise use non-partitioned ExtVP tables
    if (partitionType.toLowerCase != "predicate")
    {
     hiveDB=hiveDB.concat(partitionType)
    }

    spark.sql(s"USE $hiveDB")

    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/hive/ExtVP/$ds$partitionType.txt"),true)
//    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/hive/VT/$ds$partitionType.txt"),true)

    val queries = List(/*new ExtVPQueries q1,
		       new ExtVPQueries q2,
		       new ExtVPQueries q3,
		       new ExtVPQueries q4,
		       new ExtVPQueries q5,
		       new ExtVPQueries q6,
		       new ExtVPQueries q8,
		       new ExtVPQueries q9,
		       new ExtVPQueries q10,
		       new ExtVPQueries q11*/
		       //new VTQueries q4,
		       new ExtVPQueries q9)
		       //new VTQueries q10)


    var count = 1
    for (query <- queries)
    { 
       //run query and calculate the run time
       val starttime=System.nanoTime()
       val df=spark.sql(query)
       df.take(100).foreach(println)
       val endtime=System.nanoTime()
       val result = (endtime-starttime).toDouble/1000000000

       //write the result into the log file
       if( count != queries.size ) {
           Console.withOut(fos){print(result + ",")}
       } else {
           Console.withOut(fos){println(result)}
       }
       count+=1   
     }  
    println("All Queries are Done - HIVE - ExtVP!")

  }
}

