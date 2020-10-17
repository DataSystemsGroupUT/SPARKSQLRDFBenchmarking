package ee.ut.cs.bigdata.sp2bench.orc

import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object WPTTablesPartition {
  def main(args: Array[String]): Unit = {
    println("clouds")

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench ORC WPT")
      .getOrCreate()
    println("Spark Session is created!")

    import spark.implicits._
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase //value = {"Horizontal", "Subject", or "Predicate"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/WPT/ORC"


    //partition and save on HDFS
    if (partitionType == "subject") {
      //read tables from HDFS
      val RDFDFWPT = spark.read.format("orc").load(s"$path/WidePropertyTable.orc").toDF()
      println("Table is read!")

      RDFDFWPT.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTSubject.orc")
      println("ORC WPT partitioned and saved! Subject based partitioning!")
    } else if (partitionType == "horizontal") {
      //read tables from HDFS
      val RDFDFWPT = spark.read.format("orc").load(s"$path/WidePropertyTable.orc").toDF()
      println("Table is read!")

      RDFDFWPT.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTHorizontal.orc")
      println("ORC WPT partitioned and saved! Horizontal partitioning!")
    } else if (partitionType == "predicate") {
      val RDFDFWPT = spark.read.format("orc").load(s"$path/WidePropertyTable.orc").toDF()
      println("Table is read!")
      RDFDFWPT.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTSubject.orc")
      println("ORC WPT partitioned and saved! Subject based partitioning!")
      RDFDFWPT.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTHorizontal.orc")
      println("ORC WPT partitioned and saved! Horizontal partitioning!")


      // read splitted WPT tables
      val wptType = spark.read.format("orc").load(s"$path/WidePropertyTableType.orc").toDF()
      val wptName = spark.read.format("orc").load(s"$path/WidePropertyTableName.orc").toDF()
      val wptTitle = spark.read.format("orc").load(s"$path/WidePropertyTableTitle.orc").toDF()
      val wptIssued = spark.read.format("orc").load(s"$path/WidePropertyTableIssued.orc").toDF()
      val wptCreator = spark.read.format("orc").load(s"$path/WidePropertyTableCreator.orc").toDF()
      val wptHomepage = spark.read.format("orc").load(s"$path/WidePropertyTableHomepage.orc").toDF()
      val wptSeeAlso = spark.read.format("orc").load(s"$path/WidePropertyTableSeeAlso.orc").toDF()
      val wptBooktitle = spark.read.format("orc").load(s"$path/WidePropertyTableBooktitle.orc").toDF()
      val wptPartOf = spark.read.format("orc").load(s"$path/WidePropertyTablePartOf.orc").toDF()
      val wptAbstract = spark.read.format("orc").load(s"$path/WidePropertyTableAbstract.orc").toDF()
      val wptPages = spark.read.format("orc").load(s"$path/WidePropertyTablePages.orc").toDF()
      val wptJournal = spark.read.format("orc").load(s"$path/WidePropertyTableJournal.orc").toDF()
      val wptSeries = spark.read.format("orc").load(s"$path/WidePropertyTableSeries.orc").toDF()
      val wptNumber = spark.read.format("orc").load(s"$path/WidePropertyTableNumber.orc").toDF()
      val wptNote = spark.read.format("orc").load(s"$path/WidePropertyTableNote.orc").toDF()
      val wptVolume = spark.read.format("orc").load(s"$path/WidePropertyTableVolume.orc").toDF()
      val wptSubClassOf = spark.read.format("orc").load(s"$path/WidePropertyTableSubClassOf.orc").toDF()
      val wptMonth = spark.read.format("orc").load(s"$path/WidePropertyTableMonth.orc").toDF()
      val wptIsbn = spark.read.format("orc").load(s"$path/WidePropertyTableIsbn.orc").toDF()
      val wptEditor = spark.read.format("orc").load(s"$path/WidePropertyTableEditor.orc").toDF()
      val wptPublisher = spark.read.format("orc").load(s"$path/WidePropertyTablePublisher.orc").toDF()
      val wptReferences = spark.read.format("orc").load(s"$path/WidePropertyTableReferences.orc").toDF()
      val wptCdrom = spark.read.format("orc").load(s"$path/WidePropertyTableCdrom.orc").toDF()
      println("Splitted tables are read!")

      //partition and save on HDFS
      wptType.repartition(84, $"type").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTType" + "Predicate.orc")
      wptName.repartition(84, $"name").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTName" + "Predicate.orc")
      wptTitle.repartition(84, $"title").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTTitle" + "Predicate.orc")
      wptIssued.repartition(84, $"issued").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTIssued" + "Predicate.orc")
      wptCreator.repartition(84, $"creator").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTCreator" + "Predicate.orc")
      wptHomepage.repartition(84, $"homepage").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTHomepage" + "Predicate.orc")
      wptSeeAlso.repartition(84, $"seeAlso").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTSeeAlso" + "Predicate.orc")
      wptBooktitle.repartition(84, $"booktitle").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTBooktitle" + "Predicate.orc")
      wptPartOf.repartition(84, $"partOf").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTPartOf" + "Predicate.orc")
      wptAbstract.repartition(84, $"abstract").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTAbstract" + "Predicate.orc")
      wptPages.repartition(84, $"pages").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTPages" + "Predicate.orc")
      wptJournal.repartition(84, $"journal").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTJournal" + "Predicate.orc")
      wptSeries.repartition(84, $"series").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTSeries" + "Predicate.orc")
      wptNumber.repartition(84, $"number").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTNumber" + "Predicate.orc")
      wptNote.repartition(84, $"note").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTNote" + "Predicate.orc")
      wptVolume.repartition(84, $"volume").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTVolume" + "Predicate.orc")
      wptSubClassOf.repartition(84, $"subClassOf").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTSubClassOf" + "Predicate.orc")
      wptMonth.repartition(84, $"month").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTMonth" + "Predicate.orc")
      wptIsbn.repartition(84, $"isbn").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTIsbn" + "Predicate.orc")
      wptEditor.repartition(84, $"editor").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTEditor" + "Predicate.orc")
      wptPublisher.repartition(84, $"publisher").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTPublisher" + "Predicate.orc")
      wptReferences.repartition(84, $"references").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTReferences" + "Predicate.orc")
      wptCdrom.repartition(84, $"cdrom").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/WPTCdrom" + "Predicate.orc")

      println("ORC WPT partitioned and saved! Predicate based partitioning!")
    }
  }
}