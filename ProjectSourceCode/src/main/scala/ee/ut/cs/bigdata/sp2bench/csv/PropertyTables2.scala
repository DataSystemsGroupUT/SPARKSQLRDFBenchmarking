package ee.ut.cs.bigdata.sp2bench.csv

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.PTQueries
import java.util.concurrent.TimeoutException

object PropertyTables2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench CSV PT")    
      .getOrCreate()
    
    import spark.implicits._
    val ds=args(0)			//value = {"100M", "500M, or "1B"} 
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/PT/CSV"

    //read tables from HDFS
  
    val RDFDFDocument = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Document.csv").toDF()
    RDFDFDocument.createOrReplaceTempView("Document")

    val RDFDFPublication= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Publication.csv").toDF()
    RDFDFPublication.createOrReplaceTempView("Publication")
  
    val RDFDFReference = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Reference.csv").toDF()
    RDFDFReference.createOrReplaceTempView("Reference") 

    val RDFDFVenue= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Venue.csv").toDF()
    RDFDFVenue.createOrReplaceTempView("Venue")   

    val RDFDFPerson = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Person.csv").toDF()
    RDFDFPerson.createOrReplaceTempView("Person")    

    val RDFDFVenueType = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VenueType.csv").toDF()
    RDFDFVenueType.createOrReplaceTempView("VenueType")
    
    val RDFDFPublicationType = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/PublicationType.csv").toDF()
    RDFDFPublicationType.createOrReplaceTempView("PublicationType")

    val RDFDFAuthor = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Author.csv").toDF()
    RDFDFAuthor.createOrReplaceTempView("Author")

    val RDFDFEditor = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Editor.csv").toDF()
    RDFDFEditor.createOrReplaceTempView("Editor")

    val RDFDFDocumentSeeAlso = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Document_seeAlso.csv").toDF()
    RDFDFDocumentSeeAlso.createOrReplaceTempView("Document_seeAlso")

    val RDFDFDocumentHomepage = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Document_homepage.csv").toDF()
    RDFDFDocumentHomepage.createOrReplaceTempView("Document_homepage")

    val RDFDFDocumentAbstract = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Abstract.csv").toDF()
    RDFDFDocumentAbstract.createOrReplaceTempView("Abstract")       

    //create file to write the query run time results    
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/csv/PT/$ds.txt"),true)

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
    println("All Queries are Done - CSV - PT!") 
            
  }
}
