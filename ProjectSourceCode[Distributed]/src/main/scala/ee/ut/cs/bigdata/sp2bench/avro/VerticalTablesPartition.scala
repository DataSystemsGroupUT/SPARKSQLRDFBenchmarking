package ee.ut.cs.bigdata.sp2bench.avro

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
      .appName("RDFBench Avro VT")
      .getOrCreate()
    
    import spark.implicits._      
  
    val ds=args(0)				// data size
    val partitionType = args(1).toLowerCase	// horizontal, predicate or subject
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/Avro"

    //read tables
    val RDFDFTitle = spark.read.format("avro").load(s"$path/VT/title.avro").toDF()
    val RDFDFIssued = spark.read.format("avro").load(s"$path/VT/issued.avro").toDF()
    val RDFDFType = spark.read.format("avro").load(s"$path/VT/type.avro").toDF()
    val RDFDFCreator = spark.read.format("avro").load(s"$path/VT/creator.avro").toDF()
    val RDFDFBookTitle = spark.read.format("avro").load(s"$path/VT/booktitle.avro").toDF()
    val RDFDFPartOf = spark.read.format("avro").load(s"$path/VT/partof.avro").toDF()
    val RDFDFSeeAlso = spark.read.format("avro").load(s"$path/VT/seealso.avro").toDF()
    val RDFDFPages= spark.read.format("avro").load(s"$path/VT/pages.avro").toDF()
    val RDFDFHomePage= spark.read.format("avro").load(s"$path/VT/homepage.avro").toDF()
    val RDFDFAbstract= spark.read.format("avro").load(s"$path/VT/abstract.avro").toDF()
    val RDFDFName= spark.read.format("avro").load(s"$path/VT/name.avro").toDF()
    val RDFDFJournal= spark.read.format("avro").load(s"$path/VT/injournal.avro").toDF()
    val RDFDFSubClassOf= spark.read.format("avro").load(s"$path/VT/subclassof.avro").toDF()
    val RDFDFReferencesV= spark.read.format("avro").load(s"$path/VT/references.avro").toDF()
    val RDFDFReferences= spark.read.format("avro").load(s"$path/PT/Reference.avro").toDF()
    val RDFDFEditor= spark.read.format("avro").load(s"$path/VT/editor.avro").toDF()
    val RDFDFPredicatescombined =spark.read.format("avro").load(s"$path/ST/SingleStmtTable").toDF() 


    //partition and save on HDFS
    if(partitionType == "subject")
    {
     RDFDFTitle.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/titleSubject.avro") 
     RDFDFIssued.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/issuedSubject.avro") 
     RDFDFType.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/typeSubject.avro")
     RDFDFCreator.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/creatorSubject.avro")
     RDFDFBookTitle.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/booktitleSubject.avro")
     RDFDFPartOf.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/partofSubject.avro")
     RDFDFSeeAlso.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/seealsoSubject.avro")
     RDFDFPages.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/pagesSubject.avro")
     RDFDFHomePage.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/homepageSubject.avro")
     RDFDFAbstract.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/abstractSubject.avro")
     RDFDFName.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/nameSubject.avro")
     RDFDFJournal.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/injournalSubject.avro")
     RDFDFSubClassOf.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/subclassofSubject.avro")   
     RDFDFReferencesV.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/referencesSubject.avro")
     RDFDFReferences.repartition(84, $"document").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/PT/ReferenceSubject.avro")
     RDFDFEditor.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/editorSubject.avro")
     RDFDFPredicatescombined.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ST/SingleStmtTableSubject")
     println("Avro VT partitioned and saved! Subject based partitioning!") 
    }

    else if (partitionType == "horizontal")
    {
      RDFDFTitle.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/titleHorizontal.avro")    
      RDFDFIssued.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/issuedHorizontal.avro") 
      RDFDFType.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/typeHorizontal.avro")
      RDFDFCreator.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/creatorHorizontal.avro")
      RDFDFBookTitle.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/booktitleHorizontal.avro")
      RDFDFPartOf.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/partofHorizontal.avro")
      RDFDFSeeAlso.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/seealsoHorizontal.avro")
      RDFDFPages.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/pagesHorizontal.avro")
      RDFDFHomePage.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/homepageHorizontal.avro")
      RDFDFAbstract.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/abstractHorizontal.avro")
      RDFDFName.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/nameHorizontal.avro")
      RDFDFJournal.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/injournalHorizontal.avro")
      RDFDFSubClassOf.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/subclassofHorizontal.avro")
      RDFDFReferencesV.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/referencesHorizontal.avro")
      RDFDFReferences.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/PT/ReferenceHorizontal.avro")
      RDFDFEditor.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VT/editorHorizontal.avro")
      RDFDFPredicatescombined.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ST/SingleStmtTableHorizontal")
      println("Avro VT partitioned and saved! Horizontal partitioning!") 
   }
     
  }
}
