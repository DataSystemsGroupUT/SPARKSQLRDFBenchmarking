package ee.ut.cs.bigdata.sp2bench.hive

import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException
import ee.ut.cs.bigdata.sp2bench.queries.PTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PropertyTables {
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
    var partitionType=args(1)		//value = {"Horizontal", "Subject", or "Predicate"}
  

    //use partitioned db
    var hiveDB= db.concat(ds)
    hiveDB=hiveDB.concat(partitionType)
    spark.sql(s"USE $hiveDB")

    if (partitionType.toLowerCase == "predicate")       
    {
     // read splitted Document and Publication tables
     val document1 = spark.sql("SELECT * FROM documentbooktitle").toDF()
     val document2 = spark.sql("SELECT * FROM documentisbn").toDF()
     val document3 = spark.sql("SELECT * FROM documentissued").toDF()
     val document4 = spark.sql("SELECT * FROM documentmonth").toDF()
     val document5 = spark.sql("SELECT * FROM documentnumber").toDF()
     val document6 = spark.sql("SELECT * FROM documentpublisher").toDF()
     val document7 = spark.sql("SELECT * FROM documentseries").toDF()
     val document8 = spark.sql("SELECT * FROM documenttitle").toDF()
     val document9 = spark.sql("SELECT * FROM documentvolume").toDF()
     val publication1 = spark.sql("SELECT * FROM publicationchapter").toDF()
     val publication2 = spark.sql("SELECT * FROM publicationnote").toDF()
     val publication3 = spark.sql("SELECT * FROM publicationpages").toDF()
     val publication4 = spark.sql("SELECT * FROM publicationvenue").toDF()

     //join document tables based on 'document' column
     val document_join1 = document1.join(document2, document1("document")===document2("document")).drop(document2("document"))
     val document_join2 = document_join1.join(document3, document_join1("document")===document3("document")).drop(document3("document"))
     val document_join3 = document_join2.join(document4, document_join2("document")===document4("document")).drop(document4("document"))
     val document_join4 = document_join3.join(document5, document_join3("document")===document5("document")).drop(document5("document"))
     val document_join5 = document_join4.join(document6, document_join4("document")===document6("document")).drop(document6("document"))
     val document_join6 = document_join5.join(document7, document_join5("document")===document7("document")).drop(document7("document"))
     val document_join7 = document_join6.join(document8, document_join6("document")===document8("document")).drop(document8("document"))
     val document_join8 = document_join7.join(document9, document_join7("document")===document9("document")).drop(document9("document"))
     
     //create view
     document_join8.createOrReplaceTempView("Document")

     //join Publication documents on 'publication' column
     val publication_join1 = publication1.join(publication2, publication1("publication")===publication2("publication")).drop(publication2("publication"))
     val publication_join2 = publication_join1.join(publication3, publication_join1("publication")===publication3("publication")).drop(publication3("publication"))
     val publication_join3 = publication_join2.join(publication4, publication_join2("publication")===publication4("publication")).drop(publication4("publication"))

     //create view
     publication_join3.createOrReplaceTempView("Publication")
    }

    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/hive/PT/$ds$partitionType.txt"),true)

    val queries = List(new PTQueries q1,
		       new PTQueries q2,
		       new PTQueries q3, 
                       new PTQueries q4, 
                       new PTQueries q5, 
                       //new PTQueries q6,
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
