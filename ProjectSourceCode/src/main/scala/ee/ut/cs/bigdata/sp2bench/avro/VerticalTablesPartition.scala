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
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds"

    //read tables
    val RDFDFTitle = spark.read.format("avro").load(s"$path/VP/Avro/title.avro").toDF()
    val RDFDFIssued = spark.read.format("avro").load(s"$path/VP/Avro/issued.avro").toDF()
    val RDFDFType = spark.read.format("avro").load(s"$path/VP/Avro/type.avro").toDF()
    val RDFDFCreator = spark.read.format("avro").load(s"$path/VP/Avro/creator.avro").toDF()
    val RDFDFBookTitle = spark.read.format("avro").load(s"$path/VP/Avro/booktitle.avro").toDF()
    val RDFDFPartOf = spark.read.format("avro").load(s"$path/VP/Avro/partOf.avro").toDF()
    val RDFDFSeeAlso = spark.read.format("avro").load(s"$path/VP/Avro/seeAlso.avro").toDF()
    val RDFDFPages= spark.read.format("avro").load(s"$path/VP/Avro/pages.avro").toDF()
    val RDFDFHomePage= spark.read.format("avro").load(s"$path/VP/Avro/homepage.avro").toDF()
    val RDFDFAbstract= spark.read.format("avro").load(s"$path/VP/Avro/abstract.avro").toDF()
    val RDFDFName= spark.read.format("avro").load(s"$path/VP/Avro/name.avro").toDF()
    val RDFDFJournal= spark.read.format("avro").load(s"$path/VP/Avro/journal.avro").toDF()
    val RDFDFSubClassOf= spark.read.format("avro").load(s"$path/VP/Avro/subClassOf.avro").toDF()
    val RDFDFReferencesV= spark.read.format("avro").load(s"$path/VP/Avro/references.avro").toDF()
    val RDFDFReferences= spark.read.format("avro").load(s"$path/PT/Avro/Reference.avro").toDF()
    val RDFDFEditor= spark.read.format("avro").load(s"$path/VP/Avro/editor.avro").toDF()
    val RDFDFPredicatescombined =spark.read.format("avro").load(s"$path/ST/Avro/SingleStmtTable.avro").toDF()  


    //partition and save on HDFS
    if(partitionType == "subject")
    {
     RDFDFTitle.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/titleSubject.avro") 
     RDFDFIssued.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/issuedSubject.avro") 
     RDFDFType.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/typeSubject.avro")
     RDFDFCreator.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/creatorSubject.avro")
     RDFDFBookTitle.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/booktitleSubject.avro")
     RDFDFPartOf.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/partofSubject.avro")
     RDFDFSeeAlso.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/seealsoSubject.avro")
     RDFDFPages.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/pagesSubject.avro")
     RDFDFHomePage.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/homepageSubject.avro")
     RDFDFAbstract.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/abstractSubject.avro")
     RDFDFName.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/nameSubject.avro")
     RDFDFJournal.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/injournalSubject.avro")
     RDFDFSubClassOf.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/subclassofSubject.avro")   
     RDFDFReferencesV.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/referencesSubject.avro")
     RDFDFReferences.repartition(84, $"document").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/PT/Avro/ReferenceSubject.avro")
     RDFDFEditor.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/editorSubject.avro")
     RDFDFPredicatescombined.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ST/Avro/SingleStmtTableSubject")
     println("Avro VT partitioned and saved! Subject based partitioning!") 
    }

    else if (partitionType == "horizontal")
    {
      RDFDFTitle.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/titleHorizontal.avro")    
      RDFDFIssued.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/issuedHorizontal.avro") 
      RDFDFType.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/typeHorizontal.avro")
      RDFDFCreator.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/creatorHorizontal.avro")
      RDFDFBookTitle.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/booktitleHorizontal.avro")
      RDFDFPartOf.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/partofHorizontal.avro")
      RDFDFSeeAlso.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/seealsoHorizontal.avro")
      RDFDFPages.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/pagesHorizontal.avro")
      RDFDFHomePage.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/homepageHorizontal.avro")
      RDFDFAbstract.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/abstractHorizontal.avro")
      RDFDFName.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/nameHorizontal.avro")
      RDFDFJournal.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/injournalHorizontal.avro")
      RDFDFSubClassOf.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/subclassofHorizontal.avro")
      RDFDFReferencesV.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/referencesHorizontal.avro")
      RDFDFReferences.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/ReferenceHorizontal.avro")
      RDFDFEditor.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/editorHorizontal.avro")
      RDFDFPredicatescombined.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ST/Avro/SingleStmtTableHorizontal")
      println("Avro VT partitioned and saved! Horizontal partitioning!") 
   }
     
  }
}
