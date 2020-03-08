package ee.ut.cs.bigdata.sp2bench.csv

import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import ee.ut.cs.bigdata.sp2bench.queries.PTQueries

object PropertyTablesPartition {
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
    val ds=args(0)				//value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase	//value = {"Horizontal", "Subject", or "Predicate"}
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/CSV/PT"

    //read tables from HDFS
    val RDFDFDocument= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Document.csv").toDF()
    val RDFDFPublication= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Publication.csv").toDF()
    val RDFDFReference = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Reference.csv").toDF()   
    val RDFDFVenue= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Venue.csv").toDF()  
    val RDFDFPerson = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Person.csv").toDF()   
    val RDFDFVenueType = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VenueType.csv").toDF()
    val RDFDFPublicationType = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/PublicationType.csv").toDF()
    val RDFDFAuthor = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Author.csv").toDF()
    val RDFDFEditor = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Editor.csv").toDF()
    val RDFDFDocumentSeeAlso = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Document_seeAlso.csv").toDF()
    val RDFDFDocumentHomepage = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Document_homepage.csv").toDF()
    val RDFDFDocumentAbstract = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Abstract.csv").toDF()


    //partition and save on HDFS
    if (partitionType == "subject")
    {
     RDFDFDocument.repartition(84, $"document").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/DocumentSubject.csv")     
     RDFDFPublication.repartition(84, $"publication").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/PublicationSubject.csv")  
     RDFDFReference.repartition(84, $"document").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ReferenceSubject.csv")  
     RDFDFVenue.repartition(84, $"Venue").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VenueSubject.csv")  
     RDFDFPerson.repartition(84, $"person").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/PersonSubject.csv")  
     RDFDFVenueType.repartition(84, $"document").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VenueTypeSubject.csv")  
     RDFDFPublicationType.repartition(84, $"publication").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/PublicationTypeSubject.csv")  
     RDFDFAuthor.repartition(84, $"document").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/AuthorSubject.csv") //document
     RDFDFEditor.repartition(84, $"document").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/EditorSubject.csv") //document
     RDFDFDocumentSeeAlso.repartition(84, $"document").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/Document_seeAlsoSubject.csv")   
     RDFDFDocumentHomepage.repartition(84, $"document").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/Document_homepageSubject.csv")  
     RDFDFDocumentAbstract.repartition(84, $"publication").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/AbstractSubject.csv")  
     println("CSV PT partitioned and saved! Subject based partitioning!")
    }

    else if (partitionType == "horizontal")
    {     
     RDFDFDocument.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/DocumentHorizontal.csv")    
     RDFDFPublication.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/PublicationHorizontal.csv") 
     RDFDFReference.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ReferenceHorizontal.csv")
     RDFDFVenue.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VenueHorizontal.csv")
     RDFDFPerson.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/PersonHorizontal.csv")
     RDFDFVenueType.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VenueTypeHorizontal.csv")
     RDFDFPublicationType.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/PublicationTypeHorizontal.csv")
     RDFDFAuthor.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/AuthorHorizontal.csv")
     RDFDFEditor.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/EditorHorizontal.csv")
     RDFDFDocumentSeeAlso.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/Document_seeAlsoHorizontal.csv")
     RDFDFDocumentHomepage.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/Document_homepageHorizontal.csv")
     RDFDFDocumentAbstract.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/AbstractHorizontal.csv")
     println("CSV PT partitioned and saved! Horizontal partitioning!")
   } 
   
   else if (partitionType == "predicate")
   {
     // read splitted Document and Publication tables
     val DocumentBooktitle= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/DocumentBooktitle.csv").toDF()
     val DocumentIsbn= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/DocumentIsbn.csv").toDF()
     val DocumentIssued= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/DocumentIssued.csv").toDF()
     val DocumentMonth= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/DocumentMonth.csv").toDF()
     val DocumentNumber= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/DocumentNumber.csv").toDF()
     val DocumentPublisher= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/DocumentPublisher.csv").toDF()
     val DocumentSeries= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/DocumentSeries.csv").toDF()
     val DocumentTitle= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/DocumentTitle.csv").toDF()
     val DocumentVolume= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/DocumentVolume.csv").toDF()
     val PublicationChapter= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/PublicationChapter.csv").toDF()
     val PublicationNote= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/PublicationNote.csv").toDF()
     val PublicationPages= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/PublicationPages.csv").toDF()
     val PublicationVenue= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/PublicationVenue.csv").toDF()

     //partition and save on HDFS
     DocumentBooktitle.repartition(84, $"booktitle").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/DocumentBooktitlePredicate.csv")    
     DocumentIsbn.repartition(84, $"isbn").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/DocumentIsbnPredicate.csv")    
     DocumentIssued.repartition(84, $"issued").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/DocumentIssuedPredicate.csv")    
     DocumentMonth.repartition(84, $"month").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/DocumentMonthPredicate.csv")    
     DocumentNumber.repartition(84, $"number").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/DocumentNumberPredicate.csv")    
     DocumentPublisher.repartition(84, $"publisher").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/DocumentPublisherPredicate.csv")    
     DocumentSeries.repartition(84, $"series").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/DocumentSeriesPredicate.csv")    
     DocumentTitle.repartition(84, $"title").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/DocumentTitlePredicate.csv")    
     DocumentVolume.repartition(84, $"volume").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/DocumentVolumePredicate.csv")    

     PublicationChapter.repartition(84, $"chapter").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/PublicationChapterPredicate.csv")    
     PublicationNote.repartition(84, $"note").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/PublicationNotePredicate.csv")    
     PublicationPages.repartition(84, $"pages").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/PublicationPagesPredicate.csv")    
     PublicationVenue.repartition(84, $"venue").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/PublicationVenuePredicate.csv")    

     RDFDFReference.repartition(84, $"cited").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ReferencePredicate.csv") 
     RDFDFVenue.repartition(84, $"title").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VenuePredicate.csv") 
     RDFDFPerson.repartition(84, $"name").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/PersonPredicate.csv")  
     RDFDFVenueType.repartition(84, $"type").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VenueTypePredicate.csv") 
     RDFDFPublicationType.repartition(84, $"type").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/PublicationTypePredicate.csv") 
     RDFDFAuthor.repartition(84, $"person").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/AuthorPredicate.csv")  
     RDFDFEditor.repartition(84, $"person").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/EditorPredicate.csv")  
     RDFDFDocumentSeeAlso.repartition(84, $"seeAlso").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/Document_seeAlsoPredicate.csv") 
     RDFDFDocumentHomepage.repartition(84, $"homepage").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/Document_homepagePredicate.csv") 
     RDFDFDocumentAbstract.repartition(84, $"txt").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/AbstractPredicate.csv")  

     println("CSV PT partitioned and saved! Predicate based partitioning!")
    } 
            
  }
}


