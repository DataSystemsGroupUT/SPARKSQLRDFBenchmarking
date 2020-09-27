package ee.ut.cs.bigdata.sp2bench.orc

import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException
import ee.ut.cs.bigdata.sp2bench.queries.PTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object PropertyTablesPartition {
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

    val ds=args(0)						// data size
    val partitionType = args(1).toLowerCase	// horizontal, predicate or subject
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/ORC/PT"

    //read tables from HDFS
    val RDFDFDocument = spark.read.format("orc").load(s"$path/Document.orc").toDF()
    val RDFDFVenue= spark.read.format("orc").load(s"$path/Venue.orc").toDF()
    val RDFDFPerson = spark.read.format("orc").load(s"$path/Person.orc").toDF()
    val RDFDFPublication= spark.read.format("orc").load(s"$path/Publication.orc").toDF()
    val RDFDFVenueType = spark.read.format("orc").load(s"$path/VenueType.orc").toDF()
    val RDFDFPublicationType = spark.read.format("orc").load(s"$path/PublicationType.orc").toDF()
    val RDFDFAuthor = spark.read.format("orc").load(s"$path/Author.orc").toDF()
    val RDFDFEditor = spark.read.format("orc").load(s"$path/Editor.orc").toDF()
    val RDFDFDocumentSeeAlso = spark.read.format("orc").load(s"$path/Document_seeAlso.orc").toDF()
    val RDFDFDocumentHomepage = spark.read.format("orc").load(s"$path/Document_homepage.orc").toDF()
    val RDFDFDocumentAbstract = spark.read.format("orc").load(s"$path/Abstract.orc").toDF()
    val RDFDFReference = spark.read.format("orc").load(s"$path/Reference.orc").toDF() 


    //partition and save on HDFS
    if(partitionType == "subject")
    {
     RDFDFDocument.repartition(84, $"document").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/DocumentSubject.orc")    
     RDFDFPublication.repartition(84, $"publication").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/PublicationSubject.orc") 
     RDFDFReference.repartition(84, $"document").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ReferenceSubject.orc")
     RDFDFVenue.repartition(84, $"Venue").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VenueSubject.orc")
     RDFDFPerson.repartition(84, $"person").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/PersonSubject.orc")
     RDFDFVenueType.repartition(84, $"document").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VenueTypeSubject.orc")
     RDFDFPublicationType.repartition(84, $"publication").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/PublicationTypeSubject.orc")
     RDFDFAuthor.repartition(84, $"person").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/AuthorSubject.orc")
     RDFDFEditor.repartition(84, $"person").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/EditorSubject.orc")
     RDFDFDocumentSeeAlso.repartition(84, $"document").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/Document_seeAlsoSubject.orc")
     RDFDFDocumentHomepage.repartition(84, $"document").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/Document_homepageSubject.orc")
     RDFDFDocumentAbstract.repartition(84, $"publication").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/AbstractSubject.orc")
     println("ORC PT partitioned and saved! Subject")
    }

    else if (partitionType == "horizontal")
    {
     RDFDFDocument.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/DocumentHorizontal.orc")    
     RDFDFPublication.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/PublicationHorizontal.orc") 
     RDFDFReference.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ReferenceHorizontal.orc")
     RDFDFVenue.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VenueHorizontal.orc")
     RDFDFPerson.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/PersonHorizontal.orc")
     RDFDFVenueType.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VenueTypeHorizontal.orc")
     RDFDFPublicationType.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/PublicationTypeHorizontal.orc")
     RDFDFAuthor.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/AuthorHorizontal.orc")
     RDFDFEditor.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/EditorHorizontal.orc")
     RDFDFDocumentSeeAlso.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/Document_seeAlsoHorizontal.orc")
     RDFDFDocumentHomepage.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/Document_homepageHorizontal.orc")
     RDFDFDocumentAbstract.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/AbstractHorizontal.orc")
     println("ORC PT partitioned and saved! Horizontal")
   }

   else if (partitionType == "predicate")
   {
     println("reading orc tables")

     // read splitted Document and Publication tables
     val DocumentBooktitle= spark.read.format("orc").load(s"$path/DocumentBooktitle.orc").toDF()
     val DocumentIsbn= spark.read.format("orc").load(s"$path/DocumentIsbn.orc").toDF()
     val DocumentIssued= spark.read.format("orc").load(s"$path/DocumentIssued.orc").toDF()
     val DocumentMonth= spark.read.format("orc").load(s"$path/DocumentMonth.orc").toDF()
     val DocumentNumber= spark.read.format("orc").load(s"$path/DocumentNumber.orc").toDF()
     val DocumentPublisher= spark.read.format("orc").load(s"$path/DocumentPublisher.orc").toDF()
     val DocumentSeries= spark.read.format("orc").load(s"$path/DocumentSeries.orc").toDF()
     val DocumentTitle= spark.read.format("orc").load(s"$path/DocumentTitle.orc").toDF()
     val DocumentVolume= spark.read.format("orc").load(s"$path/DocumentVolume.orc").toDF()
     val PublicationChapter= spark.read.format("orc").load(s"$path/PublicationChapter.orc").toDF()
     val PublicationNote= spark.read.format("orc").load(s"$path/PublicationNote.orc").toDF()
     val PublicationPages= spark.read.format("orc").load(s"$path/PublicationPages.orc").toDF()
     val PublicationVenue= spark.read.format("orc").load(s"$path/PublicationVenue.orc").toDF()

     // repartition
     DocumentBooktitle.repartition(84, $"booktitle").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/DocumentBooktitlePredicate.orc")    
     DocumentIsbn.repartition(84, $"isbn").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/DocumentIsbnPredicate.orc")    
     DocumentIssued.repartition(84, $"issued").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/DocumentIssuedPredicate.orc")    
     DocumentMonth.repartition(84, $"month").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/DocumentMonthPredicate.orc")    
     DocumentNumber.repartition(84, $"number").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/DocumentNumberPredicate.orc")    
     DocumentPublisher.repartition(84, $"publisher").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/DocumentPublisherPredicate.orc")    
     DocumentSeries.repartition(84, $"series").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/DocumentSeriesPredicate.orc")    
     DocumentTitle.repartition(84, $"title").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/DocumentTitlePredicate.orc")    
     DocumentVolume.repartition(84, $"volume").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/DocumentVolumePredicate.orc")    

     PublicationChapter.repartition(84, $"chapter").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/PublicationChapterPredicate.orc")    
     PublicationNote.repartition(84, $"note").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/PublicationNotePredicate.orc")    
     PublicationPages.repartition(84, $"pages").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/PublicationPagesPredicate.orc")    
     PublicationVenue.repartition(84, $"venue").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/PublicationVenuePredicate.orc")    

     RDFDFReference.repartition(84, $"cited").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ReferencePredicate.orc")
     RDFDFVenue.repartition(84, $"title").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VenuePredicate.orc")
     RDFDFPerson.repartition(84, $"name").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/PersonPredicate.orc")
     RDFDFVenueType.repartition(84, $"type").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VenueTypePredicate.orc")
     RDFDFPublicationType.repartition(84, $"type").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/PublicationTypePredicate.orc")
     RDFDFAuthor.repartition(84, $"document").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/AuthorPredicate.orc")
     RDFDFEditor.repartition(84, $"person").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/EditorPredicate.orc")
     RDFDFDocumentSeeAlso.repartition(84, $"seeAlso").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/Document_seeAlsoPredicate.orc")
     RDFDFDocumentHomepage.repartition(84, $"homepage").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/Document_homepagePredicate.orc")
     RDFDFDocumentAbstract.repartition(84, $"txt").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/AbstractPredicate.orc")

     println("ORC PT partitioned and saved! predicate!")

    }
    
  }
}
