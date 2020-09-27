package ee.ut.cs.bigdata.sp2bench.parquet

import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException
import ee.ut.cs.bigdata.sp2bench.queries.PTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object PropertyTablesPartition{
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Parquet PT")
      .getOrCreate()

    import spark.implicits._ 

    val ds=args(0)				//value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase	//value = {"Horizontal", "Subject", or "Predicate"}
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/Parquet/PT"

    //read tables from HDFS
    val RDFDFDocument = spark.read.format("parquet").load(s"$path/Document.parquet").toDF()
    val RDFDFVenue= spark.read.format("parquet").load(s"$path/Venue.parquet").toDF()
    val RDFDFPerson = spark.read.format("parquet").load(s"$path/Person.parquet").toDF()
    val RDFDFPublication= spark.read.format("parquet").load(s"$path/Publication.parquet").toDF()
    val RDFDFVenueType = spark.read.format("parquet").load(s"$path/VenueType.parquet").toDF()
    val RDFDFPublicationType = spark.read.format("parquet").load(s"$path/PublicationType.parquet").toDF()
    val RDFDFAuthor = spark.read.format("parquet").load(s"$path/Author.parquet").toDF()
    val RDFDFEditor = spark.read.format("parquet").load(s"$path/Editor.parquet").toDF()
    val RDFDFDocumentSeeAlso = spark.read.format("parquet").load(s"$path/Document_seeAlso.parquet").toDF()
    val RDFDFDocumentHomepage = spark.read.format("parquet").load(s"$path/Document_homepage.parquet").toDF()
    val RDFDFDocumentAbstract = spark.read.format("parquet").load(s"$path/Abstract.parquet").toDF()
    val RDFDFReference = spark.read.format("parquet").load(s"$path/Reference.parquet").toDF()  

    //partition and save on HDFS
    if(partitionType == "subject")
    {
     RDFDFDocument.repartition(84, $"document").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/DocumentSubject.parquet")    
     RDFDFPublication.repartition(84, $"publication").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/PublicationSubject.parquet") 
     RDFDFReference.repartition(84, $"document").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ReferenceSubject.parquet")
     RDFDFVenue.repartition(84, $"Venue").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VenueSubject.parquet")
     RDFDFPerson.repartition(84, $"person").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/PersonSubject.parquet")
     RDFDFVenueType.repartition(84, $"document").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VenueTypeSubject.parquet")
     RDFDFPublicationType.repartition(84, $"publication").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/PublicationTypeSubject.parquet")
     RDFDFAuthor.repartition(84, $"person").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/AuthorSubject.parquet")
     RDFDFEditor.repartition(84, $"person").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/EditorSubject.parquet")
     RDFDFDocumentSeeAlso.repartition(84, $"document").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/Document_seeAlsoSubject.parquet")
     RDFDFDocumentHomepage.repartition(84, $"document").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/Document_homepageSubject.parquet")
     RDFDFDocumentAbstract.repartition(84, $"publication").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/AbstractSubject.parquet")
     println("Parquet PT partitioned and saved! Subject based Partitioning") 
    }

    else if (partitionType == "horizontal")
    {
     RDFDFDocument.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/DocumentHorizontal.parquet")    
     RDFDFPublication.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/PublicationHorizontal.parquet") 
     RDFDFReference.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ReferenceHorizontal.parquet")
     RDFDFVenue.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VenueHorizontal.parquet")
     RDFDFPerson.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/PersonHorizontal.parquet")
     RDFDFVenueType.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VenueTypeHorizontal.parquet")
     RDFDFPublicationType.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/PublicationTypeHorizontal.parquet")
     RDFDFAuthor.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/AuthorHorizontal.parquet")
     RDFDFEditor.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/EditorHorizontal.parquet")
     RDFDFDocumentSeeAlso.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/Document_seeAlsoHorizontal.parquet")
     RDFDFDocumentHomepage.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/Document_homepageHorizontal.parquet")
     RDFDFDocumentAbstract.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/AbstractHorizontal.parquet")
     println("Parquet PT partitioned and saved! Horizontal partitioning!") 
   }

   else if (partitionType.toLowerCase == "predicate")
   {
     println("reading parquet tables")

     //read splitted Document and Publication tables
     val DocumentBooktitle= spark.read.format("parquet").load(s"$path/DocumentBooktitle.parquet").toDF()
     val DocumentIsbn= spark.read.format("parquet").load(s"$path/DocumentIsbn.parquet").toDF()
     val DocumentIssued= spark.read.format("parquet").load(s"$path/DocumentIssued.parquet").toDF()
     val DocumentMonth= spark.read.format("parquet").load(s"$path/DocumentMonth.parquet").toDF()
     val DocumentNumber= spark.read.format("parquet").load(s"$path/DocumentNumber.parquet").toDF()
     val DocumentPublisher= spark.read.format("parquet").load(s"$path/DocumentPublisher.parquet").toDF()
     val DocumentSeries= spark.read.format("parquet").load(s"$path/DocumentSeries.parquet").toDF()
     val DocumentTitle= spark.read.format("parquet").load(s"$path/DocumentTitle.parquet").toDF()
     val DocumentVolume= spark.read.format("parquet").load(s"$path/DocumentVolume.parquet").toDF()
     val PublicationChapter= spark.read.format("parquet").load(s"$path/PublicationChapter.parquet").toDF()
     val PublicationNote= spark.read.format("parquet").load(s"$path/PublicationNote.parquet").toDF()
     val PublicationPages= spark.read.format("parquet").load(s"$path/PublicationPages.parquet").toDF()
     val PublicationVenue= spark.read.format("parquet").load(s"$path/PublicationVenue.parquet").toDF()

     // repartition
     DocumentBooktitle.repartition(84, $"booktitle").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/DocumentBooktitlePredicate.parquet")    
     DocumentIsbn.repartition(84, $"isbn").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/DocumentIsbnPredicate.parquet")    
     DocumentIssued.repartition(84, $"issued").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/DocumentIssuedPredicate.parquet")    
     DocumentMonth.repartition(84, $"month").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/DocumentMonthPredicate.parquet")    
     DocumentNumber.repartition(84, $"number").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/DocumentNumberPredicate.parquet")    
     DocumentPublisher.repartition(84, $"publisher").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/DocumentPublisherPredicate.parquet")    
     DocumentSeries.repartition(84, $"series").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/DocumentSeriesPredicate.parquet")    
     DocumentTitle.repartition(84, $"title").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/DocumentTitlePredicate.parquet")    
     DocumentVolume.repartition(84, $"volume").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/DocumentVolumePredicate.parquet")    

     PublicationChapter.repartition(84, $"chapter").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/PublicationChapterPredicate.parquet")    
     PublicationNote.repartition(84, $"note").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/PublicationNotePredicate.parquet")    
     PublicationPages.repartition(84, $"pages").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/PublicationPagesPredicate.parquet")    
     PublicationVenue.repartition(84, $"venue").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/PublicationVenuePredicate.parquet")    

     RDFDFReference.repartition(84, $"cited").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ReferencePredicate.parquet")
     RDFDFVenue.repartition(84, $"title").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VenuePredicate.parquet")
     RDFDFPerson.repartition(84, $"name").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/PersonPredicate.parquet")
     RDFDFVenueType.repartition(84, $"type").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VenueTypePredicate.parquet")
     RDFDFPublicationType.repartition(84, $"type").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/PublicationTypePredicate.parquet")
     RDFDFAuthor.repartition(84, $"document").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/AuthorPredicate.parquet")
     RDFDFEditor.repartition(84, $"person").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/EditorPredicate.parquet")
     RDFDFDocumentSeeAlso.repartition(84, $"seeAlso").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/Document_seeAlsoPredicate.parquet")
     RDFDFDocumentHomepage.repartition(84, $"homepage").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/Document_homepagePredicate.parquet")
     RDFDFDocumentAbstract.repartition(84, $"txt").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/AbstractPredicate.parquet")

     println("parquet PT partitioned and saved! Predicate based Partitioning!")
   } 

  }
}
