package ee.ut.cs.bigdata.sp2bench

import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SplitWPTTable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Split WPT")
      .getOrCreate()
    println("Spark Session is created!")

    import spark.implicits._
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/WPT"

    //read tables from HDFS
    val RDFDFWPT = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/CSV/WidePropertyTable.csv").toDF()
    println("WPT table is read!")

    val wptType = RDFDFWPT.select("Subject", "type")
    val wptName = RDFDFWPT.select("Subject", "name")
    val wptTitle = RDFDFWPT.select("Subject", "title")
    val wptIssued = RDFDFWPT.select("Subject", "issued")
    val wptCreator = RDFDFWPT.select("Subject", "creator")
    val wptHomepage = RDFDFWPT.select("Subject", "homepage")
    val wptSeeAlso = RDFDFWPT.select("Subject", "seeAlso")
    val wptBooktitle = RDFDFWPT.select("Subject", "booktitle")
    val wptPartOf = RDFDFWPT.select("Subject", "partOf")
    val wptAbstract = RDFDFWPT.select("Subject", "abstract")
    val wptPages = RDFDFWPT.select("Subject", "pages")
    val wptJournal = RDFDFWPT.select("Subject", "journal")
    val wptSeries = RDFDFWPT.select("Subject", "series")
    val wptNumber = RDFDFWPT.select("Subject", "number")
    val wptNote = RDFDFWPT.select("Subject", "note")
    val wptVolume = RDFDFWPT.select("Subject", "volume")
    val wptSubClassOf = RDFDFWPT.select("Subject", "subClassOf")
    val wptMonth = RDFDFWPT.select("Subject", "month")
    val wptIsbn = RDFDFWPT.select("Subject", "isbn")
    val wptEditor = RDFDFWPT.select("Subject", "editor")
    val wptPublisher = RDFDFWPT.select("Subject", "publisher")
    val wptReferences = RDFDFWPT.select("Subject", "references")
    val wptCdrom = RDFDFWPT.select("Subject", "cdrom")
    println("All selected!")

    //remove WPT from path for 100M
    wptType.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Type" + ".csv")
    wptName.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Name" + ".csv")
    wptTitle.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Title" + ".csv")
    wptIssued.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Issued" + ".csv")
    wptCreator.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Creator" + ".csv")
    wptHomepage.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Homepage" + ".csv")
    wptSeeAlso.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "SeeAlso" + ".csv")
    wptBooktitle.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Booktitle" + ".csv")
    wptPartOf.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "PartOf" + ".csv")
    wptAbstract.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Abstract" + ".csv")
    wptPages.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Pages" + ".csv")
    wptJournal.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Journal" + ".csv")
    wptSeries.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Series" + ".csv")
    wptNumber.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Number" + ".csv")
    wptNote.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Note" + ".csv")
    wptVolume.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Volume" + ".csv")
    wptSubClassOf.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "SubClassOf" + ".csv")
    wptMonth.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Month" + ".csv")
    wptIsbn.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Isbn" + ".csv")
    wptEditor.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Editor" + ".csv")
    wptPublisher.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Publisher" + ".csv")
    wptReferences.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "References" + ".csv")
    wptCdrom.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(s"$path/CSV/WidePropertyTable" + "Cdrom" + ".csv")
    println("CSV WPT tables saved!")

    wptType.write.parquet(s"$path/Parquet/WidePropertyTable" + "Type" + ".parquet")
    wptName.write.parquet(s"$path/Parquet/WidePropertyTable" + "Name" + ".parquet")
    wptTitle.write.parquet(s"$path/Parquet/WidePropertyTable" + "Title" + ".parquet")
    wptIssued.write.parquet(s"$path/Parquet/WidePropertyTable" + "Issued" + ".parquet")
    wptCreator.write.parquet(s"$path/Parquet/WidePropertyTable" + "Creator" + ".parquet")
    wptHomepage.write.parquet(s"$path/Parquet/WidePropertyTable" + "Homepage" + ".parquet")
    wptSeeAlso.write.parquet(s"$path/Parquet/WidePropertyTable" + "SeeAlso" + ".parquet")
    wptBooktitle.write.parquet(s"$path/Parquet/WidePropertyTable" + "Booktitle" + ".parquet")
    wptPartOf.write.parquet(s"$path/Parquet/WidePropertyTable" + "PartOf" + ".parquet")
    wptAbstract.write.parquet(s"$path/Parquet/WidePropertyTable" + "Abstract" + ".parquet")
    wptPages.write.parquet(s"$path/Parquet/WidePropertyTable" + "Pages" + ".parquet")
    wptJournal.write.parquet(s"$path/Parquet/WidePropertyTable" + "Journal" + ".parquet")
    wptSeries.write.parquet(s"$path/Parquet/WidePropertyTable" + "Series" + ".parquet")
    wptNumber.write.parquet(s"$path/Parquet/WidePropertyTable" + "Number" + ".parquet")
    wptNote.write.parquet(s"$path/Parquet/WidePropertyTable" + "Note" + ".parquet")
    wptVolume.write.parquet(s"$path/Parquet/WidePropertyTable" + "Volume" + ".parquet")
    wptSubClassOf.write.parquet(s"$path/Parquet/WidePropertyTable" + "SubClassOf" + ".parquet")
    wptMonth.write.parquet(s"$path/Parquet/WidePropertyTable" + "Month" + ".parquet")
    wptIsbn.write.parquet(s"$path/Parquet/WidePropertyTable" + "Isbn" + ".parquet")
    wptEditor.write.parquet(s"$path/Parquet/WidePropertyTable" + "Editor" + ".parquet")
    wptPublisher.write.parquet(s"$path/Parquet/WidePropertyTable" + "Publisher" + ".parquet")
    wptReferences.write.parquet(s"$path/Parquet/WidePropertyTable" + "References" + ".parquet")
    wptCdrom.write.parquet(s"$path/Parquet/WidePropertyTable" + "Cdrom" + ".parquet")
    println("Parquet WPT tables saved!")

    wptType.write.orc(s"$path/ORC/WidePropertyTable" + "Type" + ".orc")
    wptName.write.orc(s"$path/ORC/WidePropertyTable" + "Name" + ".orc")
    wptTitle.write.orc(s"$path/ORC/WidePropertyTable" + "Title" + ".orc")
    wptIssued.write.orc(s"$path/ORC/WidePropertyTable" + "Issued" + ".orc")
    wptCreator.write.orc(s"$path/ORC/WidePropertyTable" + "Creator" + ".orc")
    wptHomepage.write.orc(s"$path/ORC/WidePropertyTable" + "Homepage" + ".orc")
    wptSeeAlso.write.orc(s"$path/ORC/WidePropertyTable" + "SeeAlso" + ".orc")
    wptBooktitle.write.orc(s"$path/ORC/WidePropertyTable" + "Booktitle" + ".orc")
    wptPartOf.write.orc(s"$path/ORC/WidePropertyTable" + "PartOf" + ".orc")
    wptAbstract.write.orc(s"$path/ORC/WidePropertyTable" + "Abstract" + ".orc")
    wptPages.write.orc(s"$path/ORC/WidePropertyTable" + "Pages" + ".orc")
    wptJournal.write.orc(s"$path/ORC/WidePropertyTable" + "Journal" + ".orc")
    wptSeries.write.orc(s"$path/ORC/WidePropertyTable" + "Series" + ".orc")
    wptNumber.write.orc(s"$path/ORC/WidePropertyTable" + "Number" + ".orc")
    wptNote.write.orc(s"$path/ORC/WidePropertyTable" + "Note" + ".orc")
    wptVolume.write.orc(s"$path/ORC/WidePropertyTable" + "Volume" + ".orc")
    wptSubClassOf.write.orc(s"$path/ORC/WidePropertyTable" + "SubClassOf" + ".orc")
    wptMonth.write.orc(s"$path/ORC/WidePropertyTable" + "Month" + ".orc")
    wptIsbn.write.orc(s"$path/ORC/WidePropertyTable" + "Isbn" + ".orc")
    wptEditor.write.orc(s"$path/ORC/WidePropertyTable" + "Editor" + ".orc")
    wptPublisher.write.orc(s"$path/ORC/WidePropertyTable" + "Publisher" + ".orc")
    wptReferences.write.orc(s"$path/ORC/WidePropertyTable" + "References" + ".orc")
    wptCdrom.write.orc(s"$path/ORC/WidePropertyTable" + "Cdrom" + ".orc")
    println("ORC WPT tables saved!")

    wptType.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Type" + ".avro")
    wptName.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Name" + ".avro")
    wptTitle.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Title" + ".avro")
    wptIssued.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Issued" + ".avro")
    wptCreator.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Creator" + ".avro")
    wptHomepage.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Homepage" + ".avro")
    wptSeeAlso.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "SeeAlso" + ".avro")
    wptBooktitle.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Booktitle" + ".avro")
    wptPartOf.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "PartOf" + ".avro")
    wptAbstract.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Abstract" + ".avro")
    wptPages.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Pages" + ".avro")
    wptJournal.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Journal" + ".avro")
    wptSeries.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Series" + ".avro")
    wptNumber.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Number" + ".avro")
    wptNote.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Note" + ".avro")
    wptVolume.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Volume" + ".avro")
    wptSubClassOf.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "SubClassOf" + ".avro")
    wptMonth.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Month" + ".avro")
    wptIsbn.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Isbn" + ".avro")
    wptEditor.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Editor" + ".avro")
    wptPublisher.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Publisher" + ".avro")
    wptReferences.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "References" + ".avro")
    wptCdrom.write.format("avro").save(s"$path/Avro/WidePropertyTable" + "Cdrom" + ".avro")
    println("Avro WPT tables saved!")
  }
}

/*
    val filePathCSV:String = "/data/RDFBenchDatasets/250M/ST/CSV/SingleStmtTable.csv"
    val filePathAVRO:String = "/data/RDFBenchDatasets/250M/ST/AVRO/SingleStmtTable.avro"
    val RDFDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(filePathCSV).toDF()
    RDFDF.write.format("avro").save(filePathAVRO)
*/