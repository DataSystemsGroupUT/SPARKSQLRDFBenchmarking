package ee.ut.cs.bigdata.sp2bench.avro

import ee.ut.cs.bigdata.sp2bench.queries.PTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException

object PropertyTablesPartition {
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
    val ds=args(0)				//value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase	//value = {"Horizontal", "Subject", or "Predicate"}
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/Avro/PT" 		

    //read tables from HDFS
    val RDFDFDocument = spark.read.format("avro").load(s"$path/Document.avro").toDF()
    val RDFDFVenue= spark.read.format("avro").load(s"$path/Venue.avro").toDF()
    val RDFDFPerson = spark.read.format("avro").load(s"$path/Person.avro").toDF()
    val RDFDFPublication= spark.read.format("avro").load(s"$path/Publication.avro").toDF()
    val RDFDFVenueType = spark.read.format("avro").load(s"$path/VenueType.avro").toDF()
    val RDFDFPublicationType = spark.read.format("avro").load(s"$path/PublicationType.avro").toDF()
    val RDFDFAuthor = spark.read.format("avro").load(s"$path/Author.avro").toDF()
    val RDFDFEditor = spark.read.format("avro").load(s"$path/Editor.avro").toDF()
    val RDFDFDocumentSeeAlso = spark.read.format("avro").load(s"$path/Document_seeAlso.avro").toDF()
    val RDFDFDocumentHomepage = spark.read.format("avro").load(s"$path/Document_homepage.avro").toDF()
    val RDFDFDocumentAbstract = spark.read.format("avro").load(s"$path/Abstract.avro").toDF()
    val RDFDFReference = spark.read.format("avro").load(s"$path/Reference.avro").toDF() 


    //partition and save on HDFS
    if(partitionType.toLowerCase == "subject")
    {
     RDFDFDocument.repartition(84, $"document").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/DocumentSubject.avro")    
     RDFDFPublication.repartition(84, $"publication").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/PublicationSubject.avro") 
     RDFDFReference.repartition(84, $"document").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ReferenceSubject.avro")
     RDFDFVenue.repartition(84, $"Venue").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VenueSubject.avro")
     RDFDFPerson.repartition(84, $"person").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/PersonSubject.avro")
     RDFDFVenueType.repartition(84, $"document").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VenueTypeSubject.avro")
     RDFDFPublicationType.repartition(84, $"publication").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/PublicationTypeSubject.avro")
     RDFDFAuthor.repartition(84, $"person").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/AuthorSubject.avro")
     RDFDFEditor.repartition(84, $"person").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/EditorSubject.avro")
     RDFDFDocumentSeeAlso.repartition(84, $"document").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/Document_seeAlsoSubject.avro")
     RDFDFDocumentHomepage.repartition(84, $"document").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/Document_homepageSubject.avro")
     RDFDFDocumentAbstract.repartition(84, $"publication").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/AbstractSubject.avro")
     println("AVRO PT partitioned and saved! Subject based Partitioning!")
    }

    else if (partitionType == "horizontal")
    {
     RDFDFDocument.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/DocumentHorizontal.avro")    
     RDFDFPublication.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/PublicationHorizontal.avro") 
     RDFDFReference.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ReferenceHorizontal.avro")
     RDFDFVenue.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VenueHorizontal.avro")
     RDFDFPerson.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/PersonHorizontal.avro")
     RDFDFVenueType.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VenueTypeHorizontal.avro")
     RDFDFPublicationType.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/PublicationTypeHorizontal.avro")
     RDFDFAuthor.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/AuthorHorizontal.avro")
     RDFDFEditor.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/EditorHorizontal.avro")
     RDFDFDocumentSeeAlso.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/Document_seeAlsoHorizontal.avro")
     RDFDFDocumentHomepage.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/Document_homepageHorizontal.avro")
     RDFDFDocumentAbstract.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/AbstractHorizontal.avro")
     println("AVRO PT partitioned and saved! Horizontal partitioning!")
   }

   else if (partitionType.toLowerCase == "predicate")
   {     
     // read splitted Document and Publication tables
     val DocumentBooktitle= spark.read.format("avro").load(s"$path/DocumentBooktitle.avro").toDF()
     val DocumentIsbn= spark.read.format("avro").load(s"$path/DocumentIsbn.avro").toDF()
     val DocumentIssued= spark.read.format("avro").load(s"$path/DocumentIssued.avro").toDF()
     val DocumentMonth= spark.read.format("avro").load(s"$path/DocumentMonth.avro").toDF()
     val DocumentNumber= spark.read.format("avro").load(s"$path/DocumentNumber.avro").toDF()
     val DocumentPublisher= spark.read.format("avro").load(s"$path/DocumentPublisher.avro").toDF()
     val DocumentSeries= spark.read.format("avro").load(s"$path/DocumentSeries.avro").toDF()
     val DocumentTitle= spark.read.format("avro").load(s"$path/DocumentTitle.avro").toDF()
     val DocumentVolume= spark.read.format("avro").load(s"$path/DocumentVolume.avro").toDF()
     val PublicationChapter= spark.read.format("avro").load(s"$path/PublicationChapter.avro").toDF()
     val PublicationNote= spark.read.format("avro").load(s"$path/PublicationNote.avro").toDF()
     val PublicationPages= spark.read.format("avro").load(s"$path/PublicationPages.avro").toDF()
     val PublicationVenue= spark.read.format("avro").load(s"$path/PublicationVenue.avro").toDF()

     //partition and save on HDFS
     DocumentBooktitle.repartition(84, $"booktitle").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/DocumentBooktitlePredicate.avro")    
     DocumentIsbn.repartition(84, $"isbn").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/DocumentIsbnPredicate.avro")    
     DocumentIssued.repartition(84, $"issued").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/DocumentIssuedPredicate.avro")    
     DocumentMonth.repartition(84, $"month").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/DocumentMonthPredicate.avro")    
     DocumentNumber.repartition(84, $"number").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/DocumentNumberPredicate.avro")    
     DocumentPublisher.repartition(84, $"publisher").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/DocumentPublisherPredicate.avro")    
     DocumentSeries.repartition(84, $"series").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/DocumentSeriesPredicate.avro")    
     DocumentTitle.repartition(84, $"title").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/DocumentTitlePredicate.avro")    
     DocumentVolume.repartition(84, $"volume").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/DocumentVolumePredicate.avro")    

     PublicationChapter.repartition(84, $"chapter").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/PublicationChapterPredicate.avro")    
     PublicationNote.repartition(84, $"note").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/PublicationNotePredicate.avro")    
     PublicationPages.repartition(84, $"pages").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/PublicationPagesPredicate.avro")    
     PublicationVenue.repartition(84, $"venue").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/PublicationVenuePredicate.avro")    

     RDFDFReference.repartition(84, $"cited").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ReferencePredicate.avro")
     RDFDFVenue.repartition(84, $"title").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VenuePredicate.avro")
     RDFDFPerson.repartition(84, $"name").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/PersonPredicate.avro")
     RDFDFVenueType.repartition(84, $"type").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VenueTypePredicate.avro")
     RDFDFPublicationType.repartition(84, $"type").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/PublicationTypePredicate.avro")
     RDFDFAuthor.repartition(84, $"document").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/AuthorPredicate.avro")
     RDFDFEditor.repartition(84, $"person").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/EditorPredicate.avro")
     RDFDFDocumentSeeAlso.repartition(84, $"seeAlso").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/Document_seeAlsoPredicate.avro")
     RDFDFDocumentHomepage.repartition(84, $"homepage").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/Document_homepagePredicate.avro")
     RDFDFDocumentAbstract.repartition(84, $"txt").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/AbstractPredicate.avro")

     println("Avro PT partitioned and saved! Predicate based partitioning!")
   }       

  }
}
