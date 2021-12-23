package ee.ut.cs.bigdata.sp2bench.avro

import java.io.{File, FileOutputStream}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ExtVPTablesPartition {
  def main(args: Array[String]): Unit = {
    println("Avro Extvp partitioning")

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Avro ExtVP")
      .getOrCreate()

    import spark.implicits._

    val ds = args(0) // data size
    val partitionType = args(1).toLowerCase // horizontal, predicate or subject
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds"

    println("Reading Avro tables")
    //read original tables


    val vpTable9 = spark.read.format("avro").load(s"$path/VP/Avro/journal.avro").toDF()
    val vpTable11 = spark.read.format("avro").load(s"$path/VP/Avro/name.avro").toDF()


    val vpTable6 = spark.read.format("avro").load(s"$path/VP/Avro/type.avro").toDF()
    val vpTable12 = spark.read.format("avro").load(s"$path/VP/Avro/creator.avro").toDF()
    val vpTable28 = spark.read.format("avro").load(s"$path/VP/Avro/seeAlso.avro").toDF()
    val vpTable29 = spark.read.format("avro").load(s"$path/VP/Avro/editor.avro").toDF()
    val vpTable27 = spark.read.format("avro").load(s"$path/ST/Avro/SingleStmtTable.avro").toDF()
//    val vpTable27 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ST/ST/SingleStmtTable.csv").toDF()

    val vpTable1 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/type/issued.avro").toDF()
    val vpTable2 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/title/issued.avro").toDF()
    val vpTable3 = spark.read.format("avro").load(s"$path/VP/Avro/issued.avro").toDF()
    val vpTable4 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/type/pages.avro").toDF()
    val vpTable5 = spark.read.format("avro").load(s"$path/VP/Avro/pages.avro").toDF()
    val vpTable7 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SO/name/creator.avro").toDF()
    val vpTable8 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/creator/journal.avro").toDF()
    val vpTable10 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/type/journal.avro").toDF()
    val vpTable13 = spark.read.format("avro").load(s"$path/VP/Avro/abstract.avro").toDF()
    val vpTable14 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/creator/partOf.avro").toDF()
    val vpTable15 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/booktitle/seeAlso.avro").toDF()
    val vpTable16 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/partOf/seeAlso.avro").toDF()
    val vpTable17 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/homepage/partOf.avro").toDF()
    val vpTable18 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/issued/seeAlso.avro").toDF()
    val vpTable19 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/type/partOf.avro").toDF()
    val vpTable20 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/seeAlso/partOf.avro").toDF()
    val vpTable21 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/pages/partOf.avro").toDF()
    val vpTable22 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/title/partOf.avro").toDF()
    val vpTable23 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/type/creator.avro").toDF()
    val vpTable24 = spark.read.format("avro").load(s"$path/VP/Avro/subClassOf.avro").toDF()
    val vpTable25 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/issued/creator.avro").toDF()
    val vpTable26 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/creator/issued.avro").toDF()
    val vpTable30 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/type/name.avro").toDF()

    println("Reading finished!")


    //partition and save on HDFS
    if (partitionType == "subject") {
      print("Partitioning Subject based .. ")

      vpTable9.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/journalSubject.avro")
      vpTable11.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/nameSubject.avro")

      vpTable6.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/typeSubject.avro")
      vpTable12.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/creatorSubject.avro")
      vpTable27.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ST/Avro/SingleStmtTableSubject.avro")
      vpTable28.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/seeAlsoSubject.avro")
      vpTable29.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/editorSubject.avro")


      vpTable1.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/type/issuedSubject.avro")
      vpTable2.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/title/issuedSubject.avro")
      vpTable3.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/issuedSubject.avro")
      vpTable4.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/type/pagesSubject.avro")
      vpTable5.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/pagesSubject.avro")
      vpTable7.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SO/name/creatorSubject.avro")
      vpTable8.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/creator/journalSubject.avro")

      vpTable10.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/type/journalSubject.avro")

      vpTable13.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/abstractSubject.avro")
      vpTable14.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/creator/partOfSubject.avro")
      vpTable15.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/booktitle/seeAlsoSubject.avro")
      vpTable16.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/partOf/seeAlsoSubject.avro")
      vpTable17.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/homepage/partOfSubject.avro")
      vpTable18.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/issued/seeAlsoSubject.avro")
      vpTable19.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/type/partOfSubject.avro")
      vpTable20.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/seeAlso/partOfSubject.avro")
      vpTable21.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/pages/partOfSubject.avro")
      vpTable22.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/title/partOfSubject.avro")
      vpTable23.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/type/creatorSubject.avro")
      vpTable24.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/subClassOfSubject.avro")
      vpTable25.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/issued/creatorSubject.avro")
      vpTable26.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/creator/issuedSubject.avro")
      vpTable30.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/type/nameSubject.avro")
      println("Avro ExtVP partitioned and saved! Subject!")
    }
    else if (partitionType == "horizontal") {
      print("Partitioning Horizontally .. ")
      vpTable9.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/journalHorizontal.avro")
      vpTable11.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/nameHorizontal.avro")

      vpTable6.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/typeHorizontal.avro")
      vpTable12.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/creatorHorizontal.avro")
      vpTable27.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ST/Avro/SingleStmtTableHorizontal.avro")
      vpTable28.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/seeAlsoHorizontal.avro")
      vpTable29.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/editorHorizontal.avro")

      vpTable1.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/type/issuedHorizontal.avro")
      vpTable2.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/title/issuedHorizontal.avro")
      vpTable3.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/issuedHorizontal.avro")
      vpTable4.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/type/pagesHorizontal.avro")
      vpTable5.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/pagesHorizontal.avro")
      vpTable7.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SO/name/creatorHorizontal.avro")
      vpTable8.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/creator/journalHorizontal.avro")
      vpTable10.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/type/journalHorizontal.avro")
      vpTable13.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/abstractHorizontal.avro")
      vpTable14.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/creator/partOfHorizontal.avro")
      vpTable15.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/booktitle/seeAlsoHorizontal.avro")
      vpTable16.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/partOf/seeAlsoHorizontal.avro")
      vpTable17.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/homepage/partOfHorizontal.avro")
      vpTable18.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/issued/seeAlsoHorizontal.avro")
      vpTable19.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/type/partOfHorizontal.avro")
      vpTable20.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/seeAlso/partOfHorizontal.avro")
      vpTable21.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/pages/partOfHorizontal.avro")
      vpTable22.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/title/partOfHorizontal.avro")
      vpTable23.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/type/creatorHorizontal.avro")
      vpTable24.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/VP/Avro/subClassOfHorizontal.avro")
      vpTable25.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/issued/creatorHorizontal.avro")
      vpTable26.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/creator/issuedHorizontal.avro")
      vpTable30.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Avro/SS/type/nameHorizontal.avro")
      println("Avro ExtVP partitioned and saved! Horizontal!")
    }
  }
}
