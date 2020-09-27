package ee.ut.cs.bigdata.sp2bench.avro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.PTQueries
import java.util.concurrent.TimeoutException

object PropertyTables {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Avro PT")
      .getOrCreate()
        
    import spark.implicits._
    val ds=args(0)			//value = {"100M", "500M, or "1B"}  
    val partitionType = args(1)		//value = {"Horizontal", "Subject", or "Predicate"}
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/Avro/PT" 		

    //read tables from HDFS
    if (partitionType.toLowerCase == "predicate")
    {
    //read splitted Document tables
    val RDFDFDocument1 = spark.read.format("avro").load(s"$path/DocumentBooktitle$partitionType.avro").toDF()
    val RDFDFDocument2 = spark.read.format("avro").load(s"$path/DocumentIsbn$partitionType.avro").toDF()
    val RDFDFDocument3 = spark.read.format("avro").load(s"$path/DocumentIssued$partitionType.avro").toDF()
    val RDFDFDocument4 = spark.read.format("avro").load(s"$path/DocumentMonth$partitionType.avro").toDF()
    val RDFDFDocument5 = spark.read.format("avro").load(s"$path/DocumentNumber$partitionType.avro").toDF()
    val RDFDFDocument6 = spark.read.format("avro").load(s"$path/DocumentPublisher$partitionType.avro").toDF()
    val RDFDFDocument7 = spark.read.format("avro").load(s"$path/DocumentSeries$partitionType.avro").toDF()
    val RDFDFDocument8 = spark.read.format("avro").load(s"$path/DocumentTitle$partitionType.avro").toDF()
    val RDFDFDocument9 = spark.read.format("avro").load(s"$path/DocumentVolume$partitionType.avro").toDF()
  
    //join document tables based on 'document' column
    val document_join1 = RDFDFDocument1.join(RDFDFDocument2, RDFDFDocument1("document")===RDFDFDocument2("document")).drop(RDFDFDocument2("document"))
    val document_join2 = document_join1.join(RDFDFDocument3, document_join1("document")===RDFDFDocument3("document")).drop(RDFDFDocument3("document"))
    val document_join3 = document_join2.join(RDFDFDocument4, document_join2("document")===RDFDFDocument4("document")).drop(RDFDFDocument4("document"))
    val document_join4 = document_join3.join(RDFDFDocument5, document_join3("document")===RDFDFDocument5("document")).drop(RDFDFDocument5("document"))
    val document_join5 = document_join4.join(RDFDFDocument6, document_join4("document")===RDFDFDocument6("document")).drop(RDFDFDocument6("document"))
    val document_join6 = document_join5.join(RDFDFDocument7, document_join5("document")===RDFDFDocument7("document")).drop(RDFDFDocument7("document"))
    val document_join7 = document_join6.join(RDFDFDocument8, document_join6("document")===RDFDFDocument8("document")).drop(RDFDFDocument8("document"))
    val document_join8 = document_join7.join(RDFDFDocument9, document_join7("document")===RDFDFDocument9("document")).drop(RDFDFDocument9("document"))

    document_join8.createOrReplaceTempView("Document")

    //read splitted Publication tables
    val RDFDFPublication1= spark.read.format("avro").load(s"$path/PublicationChapter$partitionType.avro").toDF()
    val RDFDFPublication2= spark.read.format("avro").load(s"$path/PublicationNote$partitionType.avro").toDF()
    val RDFDFPublication3= spark.read.format("avro").load(s"$path/PublicationPages$partitionType.avro").toDF()
    val RDFDFPublication4= spark.read.format("avro").load(s"$path/PublicationVenue$partitionType.avro").toDF()

    //join Publication documents on 'publication' column
    val publication_join1 = RDFDFPublication1.join(RDFDFPublication2, RDFDFPublication1("publication")===RDFDFPublication2("publication")).drop(RDFDFPublication2("publication"))
    val publication_join2 = publication_join1.join(RDFDFPublication3, publication_join1("publication")===RDFDFPublication3("publication")).drop(RDFDFPublication3("publication"))
    val publication_join3 = publication_join2.join(RDFDFPublication4, publication_join2("publication")===RDFDFPublication4("publication")).drop(RDFDFPublication4("publication"))

    publication_join3.createOrReplaceTempView("Publication")
    }
   
    else
    {  
    val RDFDFDocument = spark.read.format("avro").load(s"$path/Document$partitionType.avro").toDF()
    RDFDFDocument.createOrReplaceTempView("Document")

    val RDFDFPublication= spark.read.format("avro").load(s"$path/Publication$partitionType.avro").toDF()
    RDFDFPublication.createOrReplaceTempView("Publication")
    }

    val RDFDFVenue= spark.read.format("avro").load(s"$path/Venue$partitionType.avro").toDF()
    RDFDFVenue.createOrReplaceTempView("Venue")

    val RDFDFPerson = spark.read.format("avro").load(s"$path/Person$partitionType.avro").toDF()
    RDFDFPerson.createOrReplaceTempView("Person")

    val RDFDFVenueType = spark.read.format("avro").load(s"$path/VenueType$partitionType.avro").toDF()
    RDFDFVenueType.createOrReplaceTempView("VenueType")

    val RDFDFPublicationType = spark.read.format("avro").load(s"$path/PublicationType$partitionType.avro").toDF()
    RDFDFPublicationType.createOrReplaceTempView("PublicationType")

    val RDFDFAuthor = spark.read.format("avro").load(s"$path/Author$partitionType.avro").toDF()
    RDFDFAuthor.createOrReplaceTempView("Author")

    val RDFDFEditor = spark.read.format("avro").load(s"$path/Editor$partitionType.avro").toDF()
    RDFDFEditor.createOrReplaceTempView("Editor")

    val RDFDFDocumentSeeAlso = spark.read.format("avro").load(s"$path/Document_seeAlso$partitionType.avro").toDF()
    RDFDFDocumentSeeAlso.createOrReplaceTempView("Document_seeAlso")

    val RDFDFDocumentHomepage = spark.read.format("avro").load(s"$path/Document_homepage$partitionType.avro").toDF()
    RDFDFDocumentHomepage.createOrReplaceTempView("Document_homepage")

    val RDFDFDocumentAbstract = spark.read.format("avro").load(s"$path/Abstract$partitionType.avro").toDF()
    RDFDFDocumentAbstract.createOrReplaceTempView("Abstract")

    val RDFDFReference = spark.read.format("avro").load(s"$path/Reference$partitionType.avro").toDF()
    RDFDFReference.createOrReplaceTempView("Reference")
    RDFDFReference.persist()        
    
    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/avro/PT/$ds$partitionType.txt"),true)           

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
           Console.withOut(fos){print(result + ",")}
       } else {
           Console.withOut(fos){println(result)}
       }
       count+=1   
    }  
    println("All Queries are Done - AVRO - PT!")   

  }
}
