package ee.ut.cs.bigdata.sp2bench.hive

import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException
import ee.ut.cs.bigdata.sp2bench.queries.PTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}

object PropertyTablesPartition {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val warehouseLocation = "hdfs://172.17.77.48:9000/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName("rdfbench Hive PT")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris","thrift://172.17.77.48:9083")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._ 
        
    val db = "rdfbench"
    val ds = args(0) 				// data size
    var partitionType=args(1).toLowerCase	// horizontal, predicate or subject

    //use original db
    var hiveDB= db.concat(ds)
    spark.sql(s"USE $hiveDB") 

    //read tables from Hive
    val RDFDFDocument = spark.sql("SELECT * FROM document").toDF()
    val RDFDFPublication= spark.sql("SELECT * FROM publication").toDF()
    val RDFDFReference = spark.sql("SELECT * FROM reference").toDF()
    val RDFDFVenue= spark.sql("SELECT * FROM venue").toDF()
    val RDFDFPerson = spark.sql("SELECT * FROM person").toDF()
    val RDFDFVenueType = spark.sql("SELECT * FROM venuetype").toDF()
    val RDFDFPublicationType = spark.sql("SELECT * FROM publicationtype").toDF()
    val RDFDFAuthor = spark.sql("SELECT * FROM author").toDF()
    val RDFDFEditor = spark.sql("SELECT * FROM editor").toDF()
    val RDFDFDocumentSeeAlso = spark.sql("SELECT * FROM document_seealso").toDF()
    val RDFDFDocumentHomepage = spark.sql("SELECT * FROM document_homepage").toDF()
    val RDFDFDocumentAbstract = spark.sql("SELECT * FROM abstract").toDF()

    //partition and save into Hive
    if(partitionType == "subject")
    {
      hiveDB = hiveDB+partitionType 
      spark.sql(s"USE $hiveDB")

      RDFDFDocument.repartition(84, $"document").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".document")
      RDFDFPublication.repartition(84, $"publication").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".publication")
      RDFDFReference.repartition(84, $"document").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".reference")
      RDFDFVenue.repartition(84, $"Venue").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".venue")
      RDFDFPerson.repartition(84, $"person").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".person")
      RDFDFVenueType.repartition(84, $"document").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".venuetype")
      RDFDFPublicationType.repartition(84, $"publication").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".publicationtype")
      RDFDFAuthor.repartition(84, $"person").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".author")
      RDFDFEditor.repartition(84, $"person").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".editor")
      RDFDFDocumentSeeAlso.repartition(84, $"document").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".document_seealso")
      RDFDFDocumentHomepage.repartition(84, $"document").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".document_homepage")
      RDFDFDocumentAbstract.repartition(84, $"publication").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".abstract")
      println("Hive PT partitioned and saved! Subject!")
    }

    else if (partitionType == "horizontal")
    {
      hiveDB = hiveDB+partitionType 
      spark.sql(s"USE $hiveDB")

      RDFDFDocument.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".document")
      RDFDFPublication.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".publication")
      RDFDFReference.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".reference")
      RDFDFVenue.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".venue")
      RDFDFPerson.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".person")
      RDFDFVenueType.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".venuetype")
      RDFDFPublicationType.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".publicationtype")
      RDFDFAuthor.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".author")
      RDFDFEditor.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".editor")
      RDFDFDocumentSeeAlso.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".document_seealso")
      RDFDFDocumentHomepage.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".document_homepage")
      RDFDFDocumentAbstract.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".abstract")   
      println("Hive PT partitioned and saved! Horizontal!")   
     }
    
    else if (partitionType.toLowerCase == "predicate")
    {
     println("reading hive tables")

     // read splitted Document and Publication tables
     val DocumentBooktitle = spark.sql("SELECT * FROM documentbooktitle").toDF()
     val DocumentIsbn      = spark.sql("SELECT * FROM documentisbn").toDF()
     val DocumentIssued    = spark.sql("SELECT * FROM documentissued").toDF()
     val DocumentMonth     = spark.sql("SELECT * FROM documentmonth").toDF()
     val DocumentNumber    = spark.sql("SELECT * FROM documentnumber").toDF()
     val DocumentPublisher = spark.sql("SELECT * FROM documentpublisher").toDF()
     val DocumentSeries    = spark.sql("SELECT * FROM documentseries").toDF()
     val DocumentTitle     = spark.sql("SELECT * FROM documenttitle").toDF()
     val DocumentVolume    = spark.sql("SELECT * FROM documentvolume").toDF()
     val PublicationChapter = spark.sql("SELECT * FROM publicationchapter").toDF()
     val PublicationNote   = spark.sql("SELECT * FROM publicationnote").toDF()
     val PublicationPages  = spark.sql("SELECT * FROM publicationpages").toDF()
     val PublicationVenue  = spark.sql("SELECT * FROM publicationvenue").toDF()

     //partition and save into Hive
     hiveDB = hiveDB+"predicate"
     spark.sql(s"USE $hiveDB")

     DocumentBooktitle.repartition(84, $"booktitle").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".documentbooktitle")    
     DocumentIsbn.repartition(84, $"isbn").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".documentisbn")    
     DocumentIssued.repartition(84, $"issued").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".documentissued")    
     DocumentMonth.repartition(84, $"month").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".documentmonth")    
     DocumentNumber.repartition(84, $"number").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".documentnumber")    
     DocumentPublisher.repartition(84, $"publisher").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".documentpublisher")    
     DocumentSeries.repartition(84, $"series").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".documentseries")    
     DocumentTitle.repartition(84, $"title").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".documenttitle")    
     DocumentVolume.repartition(84, $"volume").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".documentvolume")    

     PublicationChapter.repartition(84, $"chapter").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".publicationchapter")    
     PublicationNote.repartition(84, $"note").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".publicationnote")    
     PublicationPages.repartition(84, $"pages").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".publicationpages")    
     PublicationVenue.repartition(84, $"venue").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".publicationvenue")  


     RDFDFReference.repartition(84, $"cited").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".reference")
     RDFDFVenue.repartition(84, $"title").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".venue")
     RDFDFPerson.repartition(84, $"name").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".person")
     RDFDFVenueType.repartition(84, $"type").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".venuetype")
     RDFDFPublicationType.repartition(84, $"type").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".publicationtype")
     RDFDFAuthor.repartition(84, $"document").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".author")
     RDFDFEditor.repartition(84, $"person").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".editor")
     RDFDFDocumentSeeAlso.repartition(84, $"seeAlso").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".document_seealso")
     RDFDFDocumentHomepage.repartition(84, $"homepage").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".document_homepage")
     RDFDFDocumentAbstract.repartition(84, $"txt").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".abstract")

     println("hive PT partitioned and saved! predicate!")
    }

  }
}

