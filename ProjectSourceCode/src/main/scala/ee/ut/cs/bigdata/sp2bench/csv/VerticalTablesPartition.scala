package ee.ut.cs.bigdata.sp2bench.csv

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
      .appName("RDFBench CSV VT")
      .getOrCreate()

    import spark.implicits._

    val ds = args(0) // data size
    val partitionType = args(1).toLowerCase // horizontal, predicate or subject

    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/CSV"

    //read original tables
    val RDFDFTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/title.csv").toDF()
    val RDFDFIssued = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/issued.csv").toDF()
    val RDFDFType = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/type.csv").toDF()
    val RDFDFCreator = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/creator.csv").toDF()
    val RDFDFBookTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/booktitle.csv").toDF()
    val RDFDFPartOf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/partof.csv").toDF()
    val RDFDFSeeAlso = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/seealso.csv").toDF()
    val RDFDFPages = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/pages.csv").toDF()
    val RDFDFHomePage = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/homepage.csv").toDF()
    val RDFDFAbstract = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/abstract.csv").toDF()
    val RDFDFName = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/name.csv").toDF()
    val RDFDFJournal = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/injournal.csv").toDF()
    val RDFDFSubClassOf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/subclassof.csv").toDF()
    val RDFDFReferencesV = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/references.csv").toDF()
    val RDFDFReferences = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/PT/Reference.csv").toDF()
    val RDFDFEditor = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/editor.csv").toDF()
    val RDFDFPredicatescombined = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ST/SingleStmtTable.csv").toDF()


    //partition and save on HDFS
    if (partitionType == "subject") {
      RDFDFTitle.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/titleSubject.csv")
      RDFDFIssued.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/issuedSubject.csv")
      RDFDFType.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/typeSubject.csv")
      RDFDFCreator.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/creatorSubject.csv")
      RDFDFBookTitle.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/booktitleSubject.csv")
      RDFDFPartOf.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/partofSubject.csv")
      RDFDFSeeAlso.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/seealsoSubject.csv")
      RDFDFPages.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/pagesSubject.csv")
      RDFDFHomePage.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/homepageSubject.csv")
      RDFDFAbstract.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/abstractSubject.csv")
      RDFDFName.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/nameSubject.csv")
      RDFDFJournal.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/injournalSubject.csv")
      RDFDFSubClassOf.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/subclassofSubject.csv")
      RDFDFReferencesV.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/referencesSubject.csv")
      RDFDFReferences.repartition(84, $"document").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/PT/ReferenceSubject.csv")
      RDFDFEditor.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/editorSubject.csv")
      RDFDFPredicatescombined.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ST/SingleStmtTableSubject.csv")
      println("CSV VT partitioned and saved! Subject!")
    }

    else if (partitionType == "horizontal") {
      RDFDFTitle.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/titleHorizontal.csv")
      RDFDFIssued.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/issuedHorizontal.csv")
      RDFDFType.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/typeHorizontal.csv")
      RDFDFCreator.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/creatorHorizontal.csv")
      RDFDFBookTitle.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/booktitleHorizontal.csv")
      RDFDFPartOf.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/partofHorizontal.csv")
      RDFDFSeeAlso.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/seealsoHorizontal.csv")
      RDFDFPages.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/pagesHorizontal.csv")
      RDFDFHomePage.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/homepageHorizontal.csv")
      RDFDFAbstract.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/abstractHorizontal.csv")
      RDFDFName.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/nameHorizontal.csv")
      RDFDFJournal.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/injournalHorizontal.csv")
      RDFDFSubClassOf.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/subclassofHorizontal.csv")
      RDFDFReferencesV.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/referencesHorizontal.csv")
      RDFDFReferences.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/PT/ReferenceHorizontal.csv")
      RDFDFEditor.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VT/editorHorizontal.csv")
      RDFDFPredicatescombined.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ST/SingleStmtTableHorizontal.csv")
      println("CSV VT partitioned and saved! Horizontal!")
    }


  }
}
