package ee.ut.cs.bigdata.sp2bench.avro

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
      .appName("RDFBench Avro WPT")
      .getOrCreate()
    println("Spark Session is created!")

    import spark.implicits._
    val ds=args(0)				//value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase	//value = {"Horizontal", "Subject", or "Predicate"}
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/WPT/Avro" 


    //partition and save on HDFS
    if (partitionType == "subject")
    {
     //read tables from HDFS
     val RDFDFWPT= spark.read.format("avro").load(s"$path/WidePropertyTable.avro").toDF()
     println("Table is read!")

     RDFDFWPT.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTSubject.avro")
     println("Avro WPT partitioned and saved! Subject based partitioning!")
    }

    else if (partitionType == "horizontal")
    {
     //read tables from HDFS
     val RDFDFWPT= spark.read.format("avro").load(s"$path/WidePropertyTable.avro").toDF()
     println("Table is read!")

     RDFDFWPT.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTHorizontal.avro")
     println("Avro WPT partitioned and saved! Horizontal partitioning!")
    }
   
   else if (partitionType == "predicate")
   {

    // read splitted WPT tables
    val wptType = spark.read.format("avro").load(s"$path/WidePropertyTableType.avro").toDF()
    val wptName = spark.read.format("avro").load(s"$path/WidePropertyTableName.avro").toDF()
    val wptTitle = spark.read.format("avro").load(s"$path/WidePropertyTableTitle.avro").toDF()
    val wptIssued = spark.read.format("avro").load(s"$path/WidePropertyTableIssued.avro").toDF()
    val wptCreator = spark.read.format("avro").load(s"$path/WidePropertyTableCreator.avro").toDF()
    val wptHomepage = spark.read.format("avro").load(s"$path/WidePropertyTableHomepage.avro").toDF()
    val wptSeeAlso = spark.read.format("avro").load(s"$path/WidePropertyTableSeeAlso.avro").toDF()
    val wptBooktitle = spark.read.format("avro").load(s"$path/WidePropertyTableBooktitle.avro").toDF()
    val wptPartOf = spark.read.format("avro").load(s"$path/WidePropertyTablePartOf.avro").toDF()
    val wptAbstract = spark.read.format("avro").load(s"$path/WidePropertyTableAbstract.avro").toDF()
    val wptPages = spark.read.format("avro").load(s"$path/WidePropertyTablePages.avro").toDF()
    val wptJournal = spark.read.format("avro").load(s"$path/WidePropertyTableJournal.avro").toDF()
    val wptSeries = spark.read.format("avro").load(s"$path/WidePropertyTableSeries.avro").toDF()
    val wptNumber = spark.read.format("avro").load(s"$path/WidePropertyTableNumber.avro").toDF()
    val wptNote = spark.read.format("avro").load(s"$path/WidePropertyTableNote.avro").toDF()
    val wptVolume = spark.read.format("avro").load(s"$path/WidePropertyTableVolume.avro").toDF()
    val wptSubClassOf = spark.read.format("avro").load(s"$path/WidePropertyTableSubClassOf.avro").toDF()
    val wptMonth = spark.read.format("avro").load(s"$path/WidePropertyTableMonth.avro").toDF()
    val wptIsbn = spark.read.format("avro").load(s"$path/WidePropertyTableIsbn.avro").toDF()
    val wptEditor = spark.read.format("avro").load(s"$path/WidePropertyTableEditor.avro").toDF()
    val wptPublisher = spark.read.format("avro").load(s"$path/WidePropertyTablePublisher.avro").toDF()
    val wptReferences = spark.read.format("avro").load(s"$path/WidePropertyTableReferences.avro").toDF()
    val wptCdrom = spark.read.format("avro").load(s"$path/WidePropertyTableCdrom.avro").toDF()
    println("Splitted tables are read!")

    //partition and save on HDFS
    wptType.repartition(84, $"type").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTType" + "Predicate.avro")
    wptName.repartition(84, $"name").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTName" + "Predicate.avro")
    wptTitle.repartition(84, $"title").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTTitle" + "Predicate.avro")
    wptIssued.repartition(84, $"issued").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTIssued" + "Predicate.avro")
    wptCreator.repartition(84, $"creator").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTCreator" + "Predicate.avro")
    wptHomepage.repartition(84, $"homepage").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTHomepage" + "Predicate.avro")
    wptSeeAlso.repartition(84, $"seeAlso").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTSeeAlso" + "Predicate.avro")
    wptBooktitle.repartition(84, $"booktitle").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTBooktitle" + "Predicate.avro")
    wptPartOf.repartition(84, $"partOf").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTPartOf" + "Predicate.avro")
    wptAbstract.repartition(84, $"abstract").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTAbstract" + "Predicate.avro")
    wptPages.repartition(84, $"pages").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTPages" + "Predicate.avro")
    wptJournal.repartition(84, $"journal").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTJournal" + "Predicate.avro")
    wptSeries.repartition(84, $"series").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTSeries" + "Predicate.avro")
    wptNumber.repartition(84, $"number").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTNumber" + "Predicate.avro")
    wptNote.repartition(84, $"note").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTNote" + "Predicate.avro")
    wptVolume.repartition(84, $"volume").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTVolume" + "Predicate.avro")
    wptSubClassOf.repartition(84, $"subClassOf").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTSubClassOf" + "Predicate.avro")
    wptMonth.repartition(84, $"month").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTMonth" + "Predicate.avro")
    wptIsbn.repartition(84, $"isbn").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTIsbn" + "Predicate.avro")
    wptEditor.repartition(84, $"editor").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTEditor" + "Predicate.avro")
    wptPublisher.repartition(84, $"publisher").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTPublisher" + "Predicate.avro")
    wptReferences.repartition(84, $"references").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTReferences" + "Predicate.avro")
    wptCdrom.repartition(84, $"cdrom").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/WPTCdrom" + "Predicate.avro")

    println("Avro WPT partitioned and saved! Predicate based partitioning!")
    }
     
  }
}


