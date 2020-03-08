package ee.ut.cs.bigdata.sp2bench.orc

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
      .appName("RDFBench ORC VT")
      .getOrCreate()

    import spark.implicits._      
  
    val ds=args(0)				//value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase	//value = {"Horizontal", "Subject", or "Predicate"}      
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/ORC"

    //read tables from HDFS
    val RDFDFTitle = spark.read.format("orc").load(s"$path/VT/title.orc").toDF()
    val RDFDFIssued = spark.read.format("orc").load(s"$path/VT/issued.orc").toDF()
    val RDFDFType = spark.read.format("orc").load(s"$path/VT/type.orc").toDF()
    val RDFDFCreator = spark.read.format("orc").load(s"$path/VT/creator.orc").toDF()
    val RDFDFBookTitle = spark.read.format("orc").load(s"$path/VT/booktitle.orc").toDF()
    val RDFDFPartOf = spark.read.format("orc").load(s"$path/VT/partof.orc").toDF()
    val RDFDFSeeAlso = spark.read.format("orc").load(s"$path/VT/seealso.orc").toDF()
    val RDFDFPages= spark.read.format("orc").load(s"$path/VT/pages.orc").toDF()
    val RDFDFHomePage= spark.read.format("orc").load(s"$path/VT/homepage.orc").toDF()
    val RDFDFAbstract= spark.read.format("orc").load(s"$path/VT/abstract.orc").toDF()
    val RDFDFName= spark.read.format("orc").load(s"$path/VT/name.orc").toDF()
    val RDFDFJournal= spark.read.format("orc").load(s"$path/VT/injournal.orc").toDF()
    val RDFDFSubClassOf= spark.read.format("orc").load(s"$path/VT/subclassof.orc").toDF()
    val RDFDFReferencesV= spark.read.format("orc").load(s"$path/VT/references.orc").toDF()
    val RDFDFReferences= spark.read.format("orc").load(s"$path/PT/Reference.orc").toDF()
    val RDFDFEditor= spark.read.format("orc").load(s"$path/VT/editor.orc").toDF()
    val RDFDFPredicatescombined= spark.read.format("orc").load(s"$path/ST/SingleStmtTable").toDF() 

    //partition and save on HDFS
    if(partitionType == "subject")
    {
     RDFDFTitle.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/titleSubject.orc")    
     RDFDFIssued.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/issuedSubject.orc") 
     RDFDFType.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/typeSubject.orc")
     RDFDFCreator.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/creatorSubject.orc")
     RDFDFBookTitle.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/booktitleSubject.orc")
     RDFDFPartOf.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/partofSubject.orc")
     RDFDFSeeAlso.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/seealsoSubject.orc")
     RDFDFPages.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/pagesSubject.orc")
     RDFDFHomePage.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/homepageSubject.orc")
     RDFDFAbstract.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/abstractSubject.orc")
     RDFDFName.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/nameSubject.orc")
     RDFDFJournal.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/injournalSubject.orc")
     RDFDFSubClassOf.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/subclassofSubject.orc")
     RDFDFReferencesV.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/referencesSubject.orc")
     RDFDFReferences.repartition(84, $"document").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/PT/ReferenceSubject.orc")
     RDFDFEditor.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/editorSubject.orc")
     RDFDFPredicatescombined.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ST/SingleStmtTableSubject")
     println("ORC VT partitioned and saved! Subject based Partitioning")

    }

    else if (partitionType == "horizontal")
    {
      RDFDFTitle.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/titleHorizontal.orc")    
      RDFDFIssued.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/issuedHorizontal.orc") 
      RDFDFType.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/typeHorizontal.orc")
      RDFDFCreator.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/creatorHorizontal.orc")
      RDFDFBookTitle.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/booktitleHorizontal.orc")
      RDFDFPartOf.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/partofHorizontal.orc")
      RDFDFSeeAlso.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/seealsoHorizontal.orc")
      RDFDFPages.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/pagesHorizontal.orc")
      RDFDFHomePage.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/homepageHorizontal.orc")
      RDFDFAbstract.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/abstractHorizontal.orc")
      RDFDFName.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/nameHorizontal.orc")
      RDFDFJournal.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/injournalHorizontal.orc")
      RDFDFSubClassOf.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/subclassofHorizontal.orc")
      RDFDFReferencesV.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/referencesHorizontal.orc")
      RDFDFReferences.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/PT/ReferenceHorizontal.orc")
      RDFDFEditor.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VT/editorHorizontal.orc")
      RDFDFPredicatescombined.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ST/SingleStmtTableHorizontal")
      println("ORC VT partitioned and saved! Horizontal partitioning")
   }
      
  }
}
