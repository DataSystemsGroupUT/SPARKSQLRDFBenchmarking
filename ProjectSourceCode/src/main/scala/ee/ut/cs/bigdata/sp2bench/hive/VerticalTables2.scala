package ee.ut.cs.bigdata.sp2bench.hive

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.VTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object VerticalTables2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val warehouseLocation = "hdfs://172.17.77.48:9000/user/hive/warehouse"
    val spark = SparkSession
      .builder()
      .appName("rdfbench Hive VT")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris","thrift://172.17.77.48:9083")
      .enableHiveSupport()
      .getOrCreate()
   
    import spark.implicits._    

    val db = "rdfbench"
    val ds = args(0)		//value = {"100M", "500M, or "1B"} 

    //use partitioned db
    var hiveDB= db.concat(ds)
    spark.sql(s"USE $hiveDB")

    /* testing

    println("type")
    spark.sql(" select count (*) from type").show()

    println("creator")
    spark.sql(" select count (*) from creator").show()

    println("booktitle")
    spark.sql(" select count (*) from booktitle").show()

    println("title")
    spark.sql(" select count (*) from title").show()

    println("partof")
    spark.sql(" select count (*) from partof").show()

    println("seealso")
    spark.sql(" select count (*) from seealso").show()

    println("pages")
    spark.sql(" select count (*) from pages").show()

    println("homepage")
    spark.sql(" select count (*) from homepage").show()

    println("issued")
    spark.sql(" select count (*) from issued").show()

    println("abstractv")
    spark.sql(" select count (*) from abstractv").show()

    */

    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/hive/VP/$ds.txt"),true)

    val queries = List(new VTQueries q9)
		       /*new VTQueries q10) 
		       new VTQueries q3,
		       new VTQueries q4,
		       new VTQueries q5,
		       new VTQueries q6,
		       new VTQueries q7,
		       new VTQueries q8,
		       new VTQueries q9,
		       new VTQueries q10,
		       new VTQueries q11) */


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
    println("All Queries are Done - HIVE - VT!") 

  }
}

