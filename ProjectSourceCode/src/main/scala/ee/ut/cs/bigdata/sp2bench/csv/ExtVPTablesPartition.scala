package ee.ut.cs.bigdata.sp2bench.csv

import java.io.{File, FileOutputStream}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ExtVPTablesPartition {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench CSV ExtVP")
      .getOrCreate()

    import spark.implicits._      
  
    val ds=args(0)						// data size
    val partitionType = args(1).toLowerCase			// horizontal, predicate or subject

    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds"
    
    println("Reading tables")
    //read original tables

    val vpTable9 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/journal.csv").toDF()
    val vpTable11 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/name.csv").toDF()

/*
    val vpTable6 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/type.csv").toDF()
    val vpTable12 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/creator.csv").toDF()
    val vpTable27 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ST/CSV/SingleStmtTable.csv").toDF()
    val vpTable28 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/seeAlso.csv").toDF()
    val vpTable29 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/editor.csv").toDF()


    val vpTable1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/type/issued.csv").toDF()
    val vpTable2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/title/issued.csv").toDF()
    val vpTable3 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/issued.csv").toDF()
    val vpTable4 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/type/pages.csv").toDF()
    val vpTable5 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/pages.csv").toDF()
    val vpTable7 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SO/name/creator.csv").toDF()
    val vpTable8 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/creator/journal.csv").toDF()
    val vpTable10 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/type/journal.csv").toDF()
    val vpTable13 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/abstract.csv").toDF()
    val vpTable14 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/creator/partOf.csv").toDF()
    val vpTable15 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/booktitle/seeAlso.csv").toDF()
    val vpTable16 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/partOf/seeAlso.csv").toDF()
    val vpTable17 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/homepage/partOf.csv").toDF()                    
    val vpTable18 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/issued/seeAlso.csv").toDF()
    val vpTable19 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/type/partOf.csv").toDF()
    val vpTable20 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/seeAlso/partOf.csv").toDF()
    val vpTable21 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/pages/partOf.csv").toDF()
    val vpTable22 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/title/partOf.csv").toDF()
    val vpTable23 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/type/creator.csv").toDF()
    val vpTable24 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/subClassOf.csv").toDF()
    val vpTable25 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/issued/creator.csv").toDF()
    val vpTable26 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/creator/issued.csv").toDF()
    val vpTable30 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/type/name.csv").toDF()
*/
    println("Reading finished!")
    

    //partition and save on HDFS
    if(partitionType == "subject")
    {
     print("Partitioning Subject based: ")

     vpTable9.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/journalSubject.csv")
     vpTable11.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/nameSubject.csv")

/*
     vpTable6.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/typeSubject.csv")
     vpTable12.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/creatorSubject.csv")
     vpTable27.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ST/CSV/SingleStmtTableSubject.csv")
     vpTable28.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/seeAlsoSubject.csv")
     vpTable29.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/editorSubject.csv")


     vpTable1.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/type/issuedSubject.csv") 
     vpTable2.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/title/issuedSubject.csv")
     vpTable3.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/issuedSubject.csv")
     vpTable4.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/type/pagesSubject.csv")
     vpTable5.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/pagesSubject.csv")
     vpTable7.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SO/name/creatorSubject.csv")
     vpTable8.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/creator/journalSubject.csv")
     vpTable10.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/type/journalSubject.csv")
     vpTable13.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/abstractSubject.csv")
     vpTable14.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/creator/partOfSubject.csv")
     vpTable15.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/booktitle/seeAlsoSubject.csv")
     vpTable16.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/partOf/seeAlsoSubject.csv")
     vpTable17.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/homepage/partOfSubject.csv")
     vpTable18.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/issued/seeAlsoSubject.csv")
     vpTable19.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/type/partOfSubject.csv")
     vpTable20.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/seeAlso/partOfSubject.csv")
     vpTable21.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/pages/partOfSubject.csv")
     vpTable22.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/title/partOfSubject.csv")
     vpTable23.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/type/creatorSubject.csv")
     vpTable24.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/subClassOfSubject.csv")
     vpTable25.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/issued/creatorSubject.csv")
     vpTable26.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/creator/issuedSubject.csv")
     vpTable30.repartition(84, $"Subject").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/type/nameSubject.csv")
*/     
     println("CSV ExtVP partitioned and saved! Subject!") 
    }

    else if (partitionType == "horizontal")
    {
     print("Partitioning Horizontally: ")

     vpTable9.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/journalHorizontal.csv")
     vpTable11.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/nameHorizontal.csv")

/*
     vpTable6.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/typeHorizontal.csv")
     vpTable12.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/creatorHorizontal.csv")
     vpTable27.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ST/CSV/SingleStmtTableHorizontal.csv")
     vpTable28.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/seeAlsoHorizontal.csv")
     vpTable29.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/editorHorizontal.csv")


     vpTable1.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/type/issuedHorizontal.csv")    
     vpTable2.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/title/issuedHorizontal.csv")
     vpTable3.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/issuedHorizontal.csv")
     vpTable4.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/type/pagesHorizontal.csv")
     vpTable5.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/pagesHorizontal.csv")

     vpTable7.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SO/name/creatorHorizontal.csv")
     vpTable8.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/creator/journalHorizontal.csv")
     vpTable10.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/type/journalHorizontal.csv")
     vpTable13.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/abstractHorizontal.csv")
     vpTable14.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/creator/partOfHorizontal.csv")
     vpTable15.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/booktitle/seeAlsoHorizontal.csv")
     vpTable16.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/partOf/seeAlsoHorizontal.csv")
     vpTable17.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/homepage/partOfHorizontal.csv")
     vpTable18.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/issued/seeAlsoHorizontal.csv")
     vpTable19.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/type/partOfHorizontal.csv")
     vpTable20.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/seeAlso/partOfHorizontal.csv")
     vpTable21.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/pages/partOfHorizontal.csv")
     vpTable22.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/title/partOfHorizontal.csv")
     vpTable23.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/type/creatorHorizontal.csv")
     vpTable24.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/VP/CSV/subClassOfHorizontal.csv")
     vpTable25.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/issued/creatorHorizontal.csv")
     vpTable26.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/creator/issuedHorizontal.csv")
     vpTable30.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/ExtVP/CSV/SS/type/nameHorizontal.csv")
*/
     println("CSV ExtVP partitioned and saved! Horizontal!") 
   }  


  }
}
