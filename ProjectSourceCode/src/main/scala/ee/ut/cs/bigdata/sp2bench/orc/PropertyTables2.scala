package ee.ut.cs.bigdata.sp2bench.orc

import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException
import ee.ut.cs.bigdata.sp2bench.queries.PTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object PropertyTables2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench ORC PT")
      .getOrCreate()
    

    import spark.implicits._
    val ds=args(0)			//value = {"100M", "500M, or "1B"} 
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/PT/ORC"

    val RDFDFDocument = spark.read.format("orc").load(s"$path/Document.orc").toDF()
    RDFDFDocument.createOrReplaceTempView("Document")

    val RDFDFPublication= spark.read.format("orc").load(s"$path/Publication.orc").toDF()
    RDFDFPublication.createOrReplaceTempView("Publication")

    val RDFDFVenue= spark.read.format("orc").load(s"$path/Venue.orc").toDF()
    RDFDFVenue.createOrReplaceTempView("Venue")

    val RDFDFPerson = spark.read.format("orc").load(s"$path/Person.orc").toDF()
    RDFDFPerson.createOrReplaceTempView("Person")

    val RDFDFVenueType = spark.read.format("orc").load(s"$path/VenueType.orc").toDF()
    RDFDFVenueType.createOrReplaceTempView("VenueType")

    val RDFDFPublicationType = spark.read.format("orc").load(s"$path/PublicationType.orc").toDF()
    RDFDFPublicationType.createOrReplaceTempView("PublicationType")

    val RDFDFAuthor = spark.read.format("orc").load(s"$path/Author.orc").toDF()
    RDFDFAuthor.createOrReplaceTempView("Author")

    val RDFDFEditor = spark.read.format("orc").load(s"$path/Editor.orc").toDF()
    RDFDFEditor.createOrReplaceTempView("Editor")

    val RDFDFDocumentSeeAlso = spark.read.format("orc").load(s"$path/Document_seeAlso.orc").toDF()
    RDFDFDocumentSeeAlso.createOrReplaceTempView("Document_seeAlso")

    val RDFDFDocumentHomepage = spark.read.format("orc").load(s"$path/Document_homepage.orc").toDF()
    RDFDFDocumentHomepage.createOrReplaceTempView("Document_homepage")

    val RDFDFDocumentAbstract = spark.read.format("orc").load(s"$path/Abstract.orc").toDF()
    RDFDFDocumentAbstract.createOrReplaceTempView("Abstract")

    val RDFDFReference = spark.read.format("orc").load(s"$path/Reference.orc").toDF()
    RDFDFReference.createOrReplaceTempView("Reference")

    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/orc/PT/$ds.txt"),true)    

    val queries = List( new PTQueries q1, 
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
           Console.withOut(fos){print( result + ",")}
       } else {
           Console.withOut(fos){println(result)}
       }
       count+=1   
     }                   
    println("All Queries are Done - ORC - PT!") 

  }
}
