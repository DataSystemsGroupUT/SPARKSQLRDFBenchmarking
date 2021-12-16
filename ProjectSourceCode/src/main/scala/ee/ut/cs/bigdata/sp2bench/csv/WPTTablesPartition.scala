package ee.ut.cs.bigdata.sp2bench.csv

import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object WPTTablesPartition {
  def main(args: Array[String]): Unit = {
    println("Partitioning WPT 3!!")

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench CSV WPT")
      .getOrCreate()
    println("Spark Session is created!")

    import spark.implicits._
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase //value = {"Horizontal", "Subject", or "Predicate"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/WPT/CSV"

    //partition and save on HDFS
    if (partitionType == "subject") {
      //read tables from HDFS
      val RDFDFWPT = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(s"$path/WidePropertyTable.csv")
        .toDF()
      println("Table is read!")

      RDFDFWPT
        .repartition(84, $"Subject")
        .write.option("header", "true")
        .format("csv")
        .mode(SaveMode.Overwrite)
        .save(s"$path/WPTSubject.csv")
      println("CSV WPT partitioned and saved! Subject based partitioning!")
    }

    else if (partitionType == "horizontal") {
      //read tables from HDFS
      val RDFDFWPT = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(s"$path/WidePropertyTable.csv")
        .toDF()

      println("Table is read!")

      RDFDFWPT
        .repartition(84).write
        .option("header", "true")
        .format("csv")
        .mode(SaveMode.Overwrite)
        .save(s"$path/WPTHorizontal.csv")

      println("CSV WPT partitioned and saved! Horizontal partitioning!")
    }
    else if (partitionType == "predicate") {
      println("Doing predicate")
      // read split WPT tables
      val wptType = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableType.csv").toDF()
      val wptName = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableName.csv").toDF()
      val wptTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableTitle.csv").toDF()
      val wptIssued = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableIssued.csv").toDF()
      val wptCreator = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableCreator.csv").toDF()
      val wptHomepage = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableHomepage.csv").toDF()
      val wptSeeAlso = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableSeeAlso.csv").toDF()
      val wptBooktitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableBooktitle.csv").toDF()
      val wptPartOf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTablePartOf.csv").toDF()
      val wptAbstract = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableAbstract.csv").toDF()
      val wptPages = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTablePages.csv").toDF()
      val wptJournal = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableJournal.csv").toDF()
      val wptSeries = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableSeries.csv").toDF()
      val wptNumber = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableNumber.csv").toDF()
      val wptNote = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableNote.csv").toDF()
      val wptVolume = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableVolume.csv").toDF()
      val wptSubClassOf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableSubClassOf.csv").toDF()
      val wptMonth = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableMonth.csv").toDF()
      val wptIsbn = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableIsbn.csv").toDF()
      val wptEditor = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableEditor.csv").toDF()
      val wptPublisher = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTablePublisher.csv").toDF()
      val wptReferences = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableReferences.csv").toDF()
      val wptCdrom = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTableCdrom.csv").toDF()

      //partition and save on HDFS
      wptType.repartition(84, $"type").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTType" + "Predicate.csv")
      wptName.repartition(84, $"name").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTName" + "Predicate.csv")
      wptTitle.repartition(84, $"title").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTTitle" + "Predicate.csv")
      wptIssued.repartition(84, $"issued").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTIssued" + "Predicate.csv")
      wptCreator.repartition(84, $"creator").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTCreator" + "Predicate.csv")
      wptHomepage.repartition(84, $"homepage").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTHomepage" + "Predicate.csv")
      wptSeeAlso.repartition(84, $"seeAlso").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTSeeAlso" + "Predicate.csv")
      wptBooktitle.repartition(84, $"booktitle").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTBooktitle" + "Predicate.csv")
      wptPartOf.repartition(84, $"partOf").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTPartOf" + "Predicate.csv")
      wptAbstract.repartition(84, $"abstract").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTAbstract" + "Predicate.csv")
      wptPages.repartition(84, $"pages").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTPages" + "Predicate.csv")
      wptJournal.repartition(84, $"journal").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTJournal" + "Predicate.csv")
      wptSeries.repartition(84, $"series").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTSeries" + "Predicate.csv")
      wptNumber.repartition(84, $"number").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTNumber" + "Predicate.csv")
      wptNote.repartition(84, $"note").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTNote" + "Predicate.csv")
      wptVolume.repartition(84, $"volume").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTVolume" + "Predicate.csv")
      wptSubClassOf.repartition(84, $"subClassOf").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTSubClassOf" + "Predicate.csv")
      wptMonth.repartition(84, $"month").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTMonth" + "Predicate.csv")
      wptIsbn.repartition(84, $"isbn").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTIsbn" + "Predicate.csv")
      wptEditor.repartition(84, $"editor").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTEditor" + "Predicate.csv")
      wptPublisher.repartition(84, $"publisher").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTPublisher" + "Predicate.csv")
      wptReferences.repartition(84, $"references").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTReferences" + "Predicate.csv")
      wptCdrom.repartition(84, $"cdrom").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTCdrom" + "Predicate.csv")

      println("CSV WPT partitioned and saved! Predicate based partitioning!")
    }
  }
}
