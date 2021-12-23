package ee.ut.cs.bigdata.sp2bench.orc

import java.io.{File, FileOutputStream}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ExtVPTablesPartition {
  def main(args: Array[String]): Unit = {
    println("ORC Extvp partitioning")

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Orc ExtVP")
      .getOrCreate()

    import spark.implicits._

    val ds = args(0) // data size
    val partitionType = args(1).toLowerCase // horizontal, predicate or subject
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds"

    println("Reading ORC tables")
    //read original tables

    val vpTable9 = spark.read.format("orc").load(s"$path/VP/ORC/journal.orc").toDF()
    val vpTable11 = spark.read.format("orc").load(s"$path/VP/ORC/name.orc").toDF()

    val vpTable6 = spark.read.format("orc").load(s"$path/VP/ORC/type.orc").toDF()
    val vpTable12 = spark.read.format("orc").load(s"$path/VP/ORC/creator.orc").toDF()

    val vpTable27 = spark.read.format("orc").load(s"$path/ST/ORC/SingleStmtTable.orc").toDF()
//    val vpTable27 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ST/ST/SingleStmtTable.csv").toDF()

    val vpTable28 = spark.read.format("orc").load(s"$path/VP/ORC/seeAlso.orc").toDF()
    val vpTable29 = spark.read.format("orc").load(s"$path/VP/ORC/editor.orc").toDF()

    val vpTable1 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/type/issued.orc").toDF()
    val vpTable2 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/title/issued.orc").toDF()
    val vpTable3 = spark.read.format("orc").load(s"$path/VP/ORC/issued.orc").toDF()
    val vpTable4 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/type/pages.orc").toDF()
    val vpTable5 = spark.read.format("orc").load(s"$path/VP/ORC/pages.orc").toDF()
    val vpTable7 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SO/name/creator.orc").toDF()
    val vpTable8 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/creator/journal.orc").toDF()
    val vpTable10 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/type/journal.orc").toDF()
    val vpTable13 = spark.read.format("orc").load(s"$path/VP/ORC/abstract.orc").toDF()
    val vpTable14 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/creator/partOf.orc").toDF()
    val vpTable15 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/booktitle/seeAlso.orc").toDF()
    val vpTable16 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/partOf/seeAlso.orc").toDF()
    val vpTable17 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/homepage/partOf.orc").toDF()
    val vpTable18 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/issued/seeAlso.orc").toDF()
    val vpTable19 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/type/partOf.orc").toDF()
    val vpTable20 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/seeAlso/partOf.orc").toDF()
    val vpTable21 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/pages/partOf.orc").toDF()
    val vpTable22 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/title/partOf.orc").toDF()
    val vpTable23 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/type/creator.orc").toDF()
    val vpTable24 = spark.read.format("orc").load(s"$path/VP/ORC/subClassOf.orc").toDF()
    val vpTable25 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/issued/creator.orc").toDF()
    val vpTable26 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/creator/issued.orc").toDF()
    val vpTable30 = spark.read.format("orc").load(s"$path/ExtVP/ORC/SS/type/name.orc").toDF()

    println("Reading finished!")

    //partition and save on HDFS
    if (partitionType == "subject") {
      print("Partitioning Subject based: ")

      vpTable9.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/journalSubject.orc")
      vpTable11.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/nameSubject.orc")

      vpTable6.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/typeSubject.orc")
      vpTable12.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/creatorSubject.orc")
      vpTable27.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ST/ORC/SingleStmtTableSubject.orc")
      vpTable28.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/seeAlsoSubject.orc")
      vpTable29.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/editorSubject.orc")

      vpTable1.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/type/issuedSubject.orc")
      vpTable2.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/title/issuedSubject.orc")
      vpTable3.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/issuedSubject.orc")
      vpTable4.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/type/pagesSubject.orc")
      vpTable5.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/pagesSubject.orc")
      vpTable7.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SO/name/creatorSubject.orc")
      vpTable8.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/creator/journalSubject.orc")
      vpTable10.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/type/journalSubject.orc")
      vpTable13.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/abstractSubject.orc")
      vpTable14.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/creator/partOfSubject.orc")
      vpTable15.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/booktitle/seeAlsoSubject.orc")
      vpTable16.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/partOf/seeAlsoSubject.orc")
      vpTable17.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/homepage/partOfSubject.orc")
      vpTable18.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/issued/seeAlsoSubject.orc")
      vpTable19.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/type/partOfSubject.orc")
      vpTable20.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/seeAlso/partOfSubject.orc")
      vpTable21.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/pages/partOfSubject.orc")
      vpTable22.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/title/partOfSubject.orc")
      vpTable23.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/type/creatorSubject.orc")
      vpTable24.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/subClassOfSubject.orc")
      vpTable25.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/issued/creatorSubject.orc")
      vpTable26.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/creator/issuedSubject.orc")
      vpTable30.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/type/nameSubject.orc")

      println("ORC ExtVP partitioned and saved! Subject!")
    }
    else if (partitionType == "horizontal") {
      print("Partitioning Horizontally: ")

      vpTable9.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/journalHorizontal.orc")
      vpTable11.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/nameHorizontal.orc")


      vpTable6.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/typeHorizontal.orc")
      vpTable12.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/creatorHorizontal.orc")
      vpTable27.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ST/ORC/SingleStmtTableHorizontal.orc")
      vpTable28.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/seeAlsoHorizontal.orc")
      vpTable29.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/editorHorizontal.orc")

      vpTable1.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/type/issuedHorizontal.orc")
      vpTable2.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/title/issuedHorizontal.orc")
      vpTable3.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/issuedHorizontal.orc")
      vpTable4.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/type/pagesHorizontal.orc")
      vpTable5.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/pagesHorizontal.orc")
      vpTable7.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SO/name/creatorHorizontal.orc")
      vpTable8.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/creator/journalHorizontal.orc")
      vpTable10.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/type/journalHorizontal.orc")
      vpTable13.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/abstractHorizontal.orc")
      vpTable14.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/creator/partOfHorizontal.orc")
      vpTable15.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/booktitle/seeAlsoHorizontal.orc")
      vpTable16.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/partOf/seeAlsoHorizontal.orc")
      vpTable17.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/homepage/partOfHorizontal.orc")
      vpTable18.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/issued/seeAlsoHorizontal.orc")
      vpTable19.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/type/partOfHorizontal.orc")
      vpTable20.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/seeAlso/partOfHorizontal.orc")
      vpTable21.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/pages/partOfHorizontal.orc")
      vpTable22.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/title/partOfHorizontal.orc")
      vpTable23.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/type/creatorHorizontal.orc")
      vpTable24.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/VP/ORC/subClassOfHorizontal.orc")
      vpTable25.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/issued/creatorHorizontal.orc")
      vpTable26.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/creator/issuedHorizontal.orc")
      vpTable30.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(s"$path/ExtVP/ORC/SS/type/nameHorizontal.orc")

      println("ORC ExtVP partitioned and saved! Horizontal!")
    }
  }
}
