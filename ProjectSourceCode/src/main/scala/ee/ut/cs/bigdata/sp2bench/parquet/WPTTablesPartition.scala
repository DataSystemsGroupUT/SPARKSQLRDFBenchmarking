package ee.ut.cs.bigdata.sp2bench.parquet

import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object WPTTablesPartition {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Parquet WPT")
      .getOrCreate()
    println("Spark Session is created!")

    import spark.implicits._
    val ds=args(0)				//value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase	//value = {"Horizontal", "Subject", or "Predicate"}
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/WPT/Parquet"

    //partition and save on HDFS
    if (partitionType == "subject")
    {
     val RDFDFWPT= spark.read.format("parquet").load(s"$path/WidePropertyTable.parquet").toDF()
     println("Table is read!")

     RDFDFWPT.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTSubject.parquet")  
     println("Parquet WPT partitioned and saved! Subject based partitioning!")
    }

    else if (partitionType == "horizontal")
    {
     val RDFDFWPT= spark.read.format("parquet").load(s"$path/WidePropertyTable.parquet").toDF()
     println("Table is read!")

     RDFDFWPT.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTHorizontal.parquet")
     println("Parquet WPT partitioned and saved! Horizontal partitioning!")
    }
   
   else if (partitionType == "predicate")
   {

     // read splitted WPT tables
    val wptType = spark.read.format("parquet").load(s"$path/WidePropertyTableType.parquet").toDF()
    val wptName = spark.read.format("parquet").load(s"$path/WidePropertyTableName.parquet").toDF()
    val wptTitle = spark.read.format("parquet").load(s"$path/WidePropertyTableTitle.parquet").toDF()
    val wptIssued = spark.read.format("parquet").load(s"$path/WidePropertyTableIssued.parquet").toDF()
    val wptCreator = spark.read.format("parquet").load(s"$path/WidePropertyTableCreator.parquet").toDF()
    val wptHomepage = spark.read.format("parquet").load(s"$path/WidePropertyTableHomepage.parquet").toDF()
    val wptSeeAlso = spark.read.format("parquet").load(s"$path/WidePropertyTableSeeAlso.parquet").toDF()
    val wptBooktitle = spark.read.format("parquet").load(s"$path/WidePropertyTableBooktitle.parquet").toDF()
    val wptPartOf = spark.read.format("parquet").load(s"$path/WidePropertyTablePartOf.parquet").toDF()
    val wptAbstract = spark.read.format("parquet").load(s"$path/WidePropertyTableAbstract.parquet").toDF()
    val wptPages = spark.read.format("parquet").load(s"$path/WidePropertyTablePages.parquet").toDF()
    val wptJournal = spark.read.format("parquet").load(s"$path/WidePropertyTableJournal.parquet").toDF()
    val wptSeries = spark.read.format("parquet").load(s"$path/WidePropertyTableSeries.parquet").toDF()
    val wptNumber = spark.read.format("parquet").load(s"$path/WidePropertyTableNumber.parquet").toDF()
    val wptNote = spark.read.format("parquet").load(s"$path/WidePropertyTableNote.parquet").toDF()
    val wptVolume = spark.read.format("parquet").load(s"$path/WidePropertyTableVolume.parquet").toDF()
    val wptSubClassOf = spark.read.format("parquet").load(s"$path/WidePropertyTableSubClassOf.parquet").toDF()
    val wptMonth = spark.read.format("parquet").load(s"$path/WidePropertyTableMonth.parquet").toDF()
    val wptIsbn = spark.read.format("parquet").load(s"$path/WidePropertyTableIsbn.parquet").toDF()
    val wptEditor = spark.read.format("parquet").load(s"$path/WidePropertyTableEditor.parquet").toDF()
    val wptPublisher = spark.read.format("parquet").load(s"$path/WidePropertyTablePublisher.parquet").toDF()
    val wptReferences = spark.read.format("parquet").load(s"$path/WidePropertyTableReferences.parquet").toDF()
    val wptCdrom = spark.read.format("parquet").load(s"$path/WidePropertyTableCdrom.parquet").toDF()
    println("Splitted tables are read!")

    //partition and save on HDFS
    wptType.repartition(84, $"type").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTType" + "Predicate.parquet")
    wptName.repartition(84, $"name").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTName" + "Predicate.parquet")
    wptTitle.repartition(84, $"title").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTTitle" + "Predicate.parquet")
    wptIssued.repartition(84, $"issued").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTIssued" + "Predicate.parquet")
    wptCreator.repartition(84, $"creator").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTCreator" + "Predicate.parquet")
    wptHomepage.repartition(84, $"homepage").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTHomepage" + "Predicate.parquet")
    wptSeeAlso.repartition(84, $"seeAlso").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTSeeAlso" + "Predicate.parquet")
    wptBooktitle.repartition(84, $"booktitle").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTBooktitle" + "Predicate.parquet")
    wptPartOf.repartition(84, $"partOf").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTPartOf" + "Predicate.parquet")
    wptAbstract.repartition(84, $"abstract").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTAbstract" + "Predicate.parquet")
    wptPages.repartition(84, $"pages").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTPages" + "Predicate.parquet")
    wptJournal.repartition(84, $"journal").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTJournal" + "Predicate.parquet")
    wptSeries.repartition(84, $"series").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTSeries" + "Predicate.parquet")
    wptNumber.repartition(84, $"number").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTNumber" + "Predicate.parquet")
    wptNote.repartition(84, $"note").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTNote" + "Predicate.parquet")
    wptVolume.repartition(84, $"volume").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTVolume" + "Predicate.parquet")
    wptSubClassOf.repartition(84, $"subClassOf").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTSubClassOf" + "Predicate.parquet")
    wptMonth.repartition(84, $"month").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTMonth" + "Predicate.parquet")
    wptIsbn.repartition(84, $"isbn").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTIsbn" + "Predicate.parquet")
    wptEditor.repartition(84, $"editor").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTEditor" + "Predicate.parquet")
    wptPublisher.repartition(84, $"publisher").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTPublisher" + "Predicate.parquet")
    wptReferences.repartition(84, $"references").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTReferences" + "Predicate.parquet")
    wptCdrom.repartition(84, $"cdrom").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/WPTCdrom" + "Predicate.parquet")

    println("Parquet WPT partitioned and saved! Predicate based partitioning!")
    }
     
  }
}


