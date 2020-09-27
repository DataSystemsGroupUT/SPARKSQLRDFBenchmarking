package ee.ut.cs.bigdata.sp2bench.hive

import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException
import ee.ut.cs.bigdata.sp2bench.queries.PTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PropertyTables2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val warehouseLocation = "hdfs://172.17.77.48:9000/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      //.master("spark://172.17.77.48:7077")
      .appName("rdfbench Hive PT")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris","thrift://172.17.77.48:9083")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._    
        
    val db = "rdfbench"
    val ds = args(0)			//value = {"100M", "500M, or "1B"} 

    //use partitioned db
    var hiveDB= db.concat(ds)
    spark.sql(s"USE $hiveDB")

    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/hive/PT/$ds.txt"),true)

    val queries = List(new PTQueries q1,
		       new PTQueries q2,
		       new PTQueries q3, 
                       new PTQueries q4, 
                       new PTQueries q5, 
                       new PTQueries q6,
                       new PTQueries q8,
                       new PTQueries q10,
                       new PTQueries q11)    
      
    var count = 1
    for (query <- queries)
    { 
       val starttime=System.nanoTime()
       val df=spark.sql(query)
       df.take(100).foreach(println)
       val endtime=System.nanoTime()
       val result = (endtime-starttime).toDouble/1000000000

        if( count != queries.size ) {
            Console.withOut(fos){print(result + ",")}
        } else {
            Console.withOut(fos){println(result)}
        }
        count+=1   
    }    
    println("All Queries are Done - HIVE - PT!")
    
  }
}
