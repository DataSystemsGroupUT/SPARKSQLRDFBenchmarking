package ee.ut.cs.bigdata.sp2bench.parquet

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.VTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object VerticalTablesPartition {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Parquet VT")
      .getOrCreate()

    import spark.implicits._      
  
    val ds=args(0)				//value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase	//value = {"Horizontal", "Subject", or "Predicate"}       
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/Parquet"

    //read tables from HDFS
    val RDFDFTitle = spark.read.format("parquet").load(s"$path/VT/title.parquet").toDF()
    val RDFDFIssued = spark.read.format("parquet").load(s"$path/VT/issued.parquet").toDF()
    val RDFDFType = spark.read.format("parquet").load(s"$path/VT/type.parquet").toDF()
    val RDFDFCreator = spark.read.format("parquet").load(s"$path/VT/creator.parquet").toDF()
    val RDFDFBookTitle = spark.read.format("parquet").load(s"$path/VT/booktitle.parquet").toDF()
    val RDFDFPartOf = spark.read.format("parquet").load(s"$path/VT/partof.parquet").toDF()
    val RDFDFSeeAlso = spark.read.format("parquet").load(s"$path/VT/seealso.parquet").toDF()
    val RDFDFPages= spark.read.format("parquet").load(s"$path/VT/pages.parquet").toDF()
    val RDFDFHomePage= spark.read.format("parquet").load(s"$path/VT/homepage.parquet").toDF()
    val RDFDFAbstract= spark.read.format("parquet").load(s"$path/VT/abstract.parquet").toDF()
    val RDFDFName= spark.read.format("parquet").load(s"$path/VT/name.parquet").toDF()
    val RDFDFJournal= spark.read.format("parquet").load(s"$path/VT/injournal.parquet").toDF()
    val RDFDFSubClassOf= spark.read.format("parquet").load(s"$path/VT/subclassof.parquet").toDF()
    val RDFDFReferencesV= spark.read.format("parquet").load(s"$path/VT/references.parquet").toDF()
    val RDFDFReferences= spark.read.format("parquet").load(s"$path/PT/Reference.parquet").toDF()
    val RDFDFEditor= spark.read.format("parquet").load(s"$path/VT/editor.parquet").toDF()
    val RDFDFPredicatescombined= spark.read.format("parquet").load(s"$path/ST/SingleStmtTable").toDF() 

    //partition and save on HDFS
    if(partitionType == "subject")
    {
     RDFDFTitle.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/titleSubject.parquet")    
     RDFDFIssued.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/issuedSubject.parquet") 
     RDFDFType.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/typeSubject.parquet")
     RDFDFCreator.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/creatorSubject.parquet")
     RDFDFBookTitle.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/booktitleSubject.parquet")
     RDFDFPartOf.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/partofSubject.parquet")
     RDFDFSeeAlso.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/seealsoSubject.parquet")
     RDFDFPages.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/pagesSubject.parquet")
     RDFDFHomePage.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/homepageSubject.parquet")
     RDFDFAbstract.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/abstractSubject.parquet")
     RDFDFName.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/nameSubject.parquet")
     RDFDFJournal.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/injournalSubject.parquet")
     RDFDFSubClassOf.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/subclassofSubject.parquet")
     RDFDFReferencesV.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/referencesSubject.parquet")
     RDFDFReferences.repartition(84, $"document").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/PT/ReferenceSubject.parquet")
     RDFDFEditor.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/editorSubject.parquet")
     RDFDFPredicatescombined.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ST/SingleStmtTableSubject")
     println("Parquet VT partitioned and saved! Subject based Partitioning!") 
    }

    else if (partitionType == "horizontal")
    {
      RDFDFTitle.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/titleHorizontal.parquet")    
      RDFDFIssued.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/issuedHorizontal.parquet") 
      RDFDFType.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/typeHorizontal.parquet")
      RDFDFCreator.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/creatorHorizontal.parquet")
      RDFDFBookTitle.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/booktitleHorizontal.parquet")
      RDFDFPartOf.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/partofHorizontal.parquet")
      RDFDFSeeAlso.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/seealsoHorizontal.parquet")
      RDFDFPages.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/pagesHorizontal.parquet")
      RDFDFHomePage.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/homepageHorizontal.parquet")
      RDFDFAbstract.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/abstractHorizontal.parquet")
      RDFDFName.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/nameHorizontal.parquet")
      RDFDFJournal.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/injournalHorizontal.parquet")
      RDFDFSubClassOf.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/subclassofHorizontal.parquet")
      RDFDFReferencesV.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/referencesHorizontal.parquet")
      RDFDFReferences.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/PT/ReferenceHorizontal.parquet")
      RDFDFEditor.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VT/editorHorizontal.parquet")
      RDFDFPredicatescombined.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ST/SingleStmtTableHorizontal")
      println("Parquet VT partitioned and saved! Horizontal partitioning!") 
   }
    
  }
}
