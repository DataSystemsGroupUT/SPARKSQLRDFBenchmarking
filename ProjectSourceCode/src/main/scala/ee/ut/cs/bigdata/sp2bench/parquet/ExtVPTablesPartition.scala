package ee.ut.cs.bigdata.sp2bench.parquet

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
      .appName("RDFBench Parquet ExtVP")
      .getOrCreate()

    import spark.implicits._      
  
    val ds=args(0)						// data size
    val partitionType = args(1).toLowerCase			// horizontal, predicate or subject
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds"
    
    println("Reading Parquet tables")
    //read original tables
    val vpTable9 = spark.read.format("parquet").load(s"$path/VP/Parquet/journal.parquet").toDF()
    val vpTable11 = spark.read.format("parquet").load(s"$path/VP/Parquet/name.parquet").toDF()


/*
    val vpTable6 = spark.read.format("parquet").load(s"$path/VP/Parquet/type.parquet").toDF()
    val vpTable12 = spark.read.format("parquet").load(s"$path/VP/Parquet/creator.parquet").toDF()
    val vpTable27 = spark.read.format("parquet").load(s"$path/ST/Parquet/SingleStmtTable.parquet").toDF()
    val vpTable28 = spark.read.format("parquet").load(s"$path/VP/Parquet/seeAlso.parquet").toDF()
    val vpTable29 = spark.read.format("parquet").load(s"$path/VP/Parquet/editor.parquet").toDF()

    val vpTable1 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/issued.parquet").toDF()
    val vpTable2 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/title/issued.parquet").toDF()
    val vpTable3 = spark.read.format("parquet").load(s"$path/VP/Parquet/issued.parquet").toDF()
    val vpTable4 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/pages.parquet").toDF()
    val vpTable5 = spark.read.format("parquet").load(s"$path/VP/Parquet/pages.parquet").toDF()

    val vpTable7 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SO/name/creator.parquet").toDF()
    val vpTable8 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/creator/journal.parquet").toDF()
    val vpTable10 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/journal.parquet").toDF()

    val vpTable13 = spark.read.format("parquet").load(s"$path/VP/Parquet/abstract.parquet").toDF()
    val vpTable14 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/creator/partOf.parquet").toDF()
    val vpTable15 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/booktitle/seeAlso.parquet").toDF()
    val vpTable16 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/partOf/seeAlso.parquet").toDF()
    val vpTable17 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/homepage/partOf.parquet").toDF()                    
    val vpTable18 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/issued/seeAlso.parquet").toDF()
    val vpTable19 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/partOf.parquet").toDF()
    val vpTable20 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/seeAlso/partOf.parquet").toDF()
    val vpTable21 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/pages/partOf.parquet").toDF()
    val vpTable22 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/title/partOf.parquet").toDF()
    val vpTable23 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/creator.parquet").toDF()
    val vpTable24 = spark.read.format("parquet").load(s"$path/VP/Parquet/subClassOf.parquet").toDF()
    val vpTable25 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/issued/creator.parquet").toDF()
    val vpTable26 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/creator/issued.parquet").toDF()
    val vpTable30 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/name.parquet").toDF()
*/
    println("Reading finished!")
    

    //partition and save on HDFS
    if(partitionType == "subject")
    {
     print("Partitioning Subject based: ")
     vpTable9.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/journalSubject.parquet")
     vpTable11.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/nameSubject.parquet")

/*
     vpTable6.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/typeSubject.parquet")
     vpTable12.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/creatorSubject.parquet")
     vpTable27.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ST/Parquet/SingleStmtTableSubject.parquet")
     vpTable28.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/seeAlsoSubject.parquet")
     vpTable29.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/editorSubject.parquet")

     vpTable1.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/type/issuedSubject.parquet") 
     vpTable2.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/title/issuedSubject.parquet")
     vpTable3.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/issuedSubject.parquet")
     vpTable4.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/type/pagesSubject.parquet")
     vpTable5.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/pagesSubject.parquet")
     vpTable7.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SO/name/creatorSubject.parquet")
     vpTable8.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/creator/journalSubject.parquet")
     vpTable10.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/type/journalSubject.parquet")
     vpTable13.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/abstractSubject.parquet")
     vpTable14.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/creator/partOfSubject.parquet")
     vpTable15.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/booktitle/seeAlsoSubject.parquet")
     vpTable16.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/partOf/seeAlsoSubject.parquet")
     vpTable17.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/homepage/partOfSubject.parquet")
     vpTable18.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/issued/seeAlsoSubject.parquet")
     vpTable19.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/type/partOfSubject.parquet")
     vpTable20.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/seeAlso/partOfSubject.parquet")
     vpTable21.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/pages/partOfSubject.parquet")
     vpTable22.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/title/partOfSubject.parquet")
     vpTable23.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/type/creatorSubject.parquet")
     vpTable24.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/subClassOfSubject.parquet")
     vpTable25.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/issued/creatorSubject.parquet")
     vpTable26.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/creator/issuedSubject.parquet")
     vpTable30.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/type/nameSubject.parquet")
*/
     println("Parquet ExtVP partitioned and saved! Subject!") 
    }

    else if (partitionType == "horizontal")
    {
     print("Partitioning Horizontally: ")
     vpTable9.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/journalHorizontal.parquet")
     vpTable11.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/nameHorizontal.parquet")


/*
     vpTable6.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/typeHorizontal.parquet")
     vpTable12.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/creatorHorizontal.parquet")
     vpTable27.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ST/Parquet/SingleStmtTableHorizontal.parquet")
     vpTable28.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/seeAlsoHorizontal.parquet")
     vpTable29.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/editorHorizontal.parquet")



     vpTable1.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/type/issuedHorizontal.parquet")    
     vpTable2.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/title/issuedHorizontal.parquet")
     vpTable3.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/issuedHorizontal.parquet")
     vpTable4.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/type/pagesHorizontal.parquet")
     vpTable5.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/pagesHorizontal.parquet")

     vpTable7.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SO/name/creatorHorizontal.parquet")
     vpTable8.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/creator/journalHorizontal.parquet")
     vpTable10.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/type/journalHorizontal.parquet")

     vpTable13.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/abstractHorizontal.parquet")
     vpTable14.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/creator/partOfHorizontal.parquet")
     vpTable15.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/booktitle/seeAlsoHorizontal.parquet")
     vpTable16.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/partOf/seeAlsoHorizontal.parquet")
     vpTable17.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/homepage/partOfHorizontal.parquet")
     vpTable18.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/issued/seeAlsoHorizontal.parquet")
     vpTable19.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/type/partOfHorizontal.parquet")
     vpTable20.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/seeAlso/partOfHorizontal.parquet")
     vpTable21.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/pages/partOfHorizontal.parquet")
     vpTable22.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/title/partOfHorizontal.parquet")
     vpTable23.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/type/creatorHorizontal.parquet")
     vpTable24.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/VP/Parquet/subClassOfHorizontal.parquet")
     vpTable25.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/issued/creatorHorizontal.parquet")
     vpTable26.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/creator/issuedHorizontal.parquet")
     vpTable30.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/ExtVP/Parquet/SS/type/nameHorizontal.parquet")
*/

     println("Parquet ExtVP partitioned and saved! Horizontal!") 
   }  


  }
}
