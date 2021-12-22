package ee.ut.cs.bigdata.sp2bench.csv

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.ExtVPQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ExtVPTables2 {
  def main(args: Array[String]): Unit = {
    println("ExtVP VHDFS CSV")

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench CSV ExtVP")
      .getOrCreate()

    println("Spark session is created!")

    import spark.implicits._
    val ds = args(0) // value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds"

    println("Reading Tables...")
    //read tables from HDFS

    val vpTable6 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/type.csv").toDF()
      val vpTable27 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ST/ST/SingleStmtTable.csv").toDF()
    //    val vpTable27 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ST/CSV/SingleStmtTable.csv").toDF()

    val vpTable1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/type/issued.csv").toDF()
    val vpTable2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/title/issued.csv").toDF()
    val vpTable3 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/issued.csv").toDF()
    val vpTable4 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/type/pages.csv").toDF()
    val vpTable5 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/pages.csv").toDF()

    val vpTable7 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SO/name/creator.csv").toDF()
    val vpTable8 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/creator/journal.csv").toDF()
    val vpTable9 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/journal.csv").toDF()
    val vpTable10 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/type/journal.csv").toDF()
    val vpTable11 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/name.csv").toDF()
    val vpTable12 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/creator.csv").toDF()
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

    val vpTable28 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/seeAlso.csv").toDF()
    val vpTable29 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/editor.csv").toDF()
    val vpTable30 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ExtVP/CSV/SS/type/name.csv").toDF()
    /*
    */

    vpTable6.createOrReplaceTempView("VP_Type")
    vpTable27.createOrReplaceTempView("Triples")

    vpTable1.createOrReplaceTempView("ExtVP_SS_Type_Issued")
    vpTable2.createOrReplaceTempView("ExtVP_SS_Title_Issued")
    vpTable3.createOrReplaceTempView("VP_Issued")
    vpTable4.createOrReplaceTempView("ExtVP_SS_Type_Pages")
    vpTable5.createOrReplaceTempView("VP_Pages")

    vpTable7.createOrReplaceTempView("ExtVP_SO_Name_Creator")
    vpTable8.createOrReplaceTempView("ExtVP_SS_Creator_Journal")
    vpTable9.createOrReplaceTempView("VP_Journal")
    vpTable10.createOrReplaceTempView("ExtVP_SS_Type_Journal")
    vpTable11.createOrReplaceTempView("VP_Name")
    vpTable12.createOrReplaceTempView("VP_Creator")
    vpTable13.createOrReplaceTempView("VP_Abstract")
    vpTable14.createOrReplaceTempView("ExtVP_SS_Creator_PartOf")
    vpTable15.createOrReplaceTempView("ExtVP_SS_BookTitle_SeeAlso")
    vpTable16.createOrReplaceTempView("ExtVP_SS_PartOf_SeeAlso")
    vpTable17.createOrReplaceTempView("ExtVP_SS_HomePage_PartOf")
    vpTable18.createOrReplaceTempView("ExtVP_SS_Issued_SeeAlso")
    vpTable19.createOrReplaceTempView("ExtVP_SS_Type_PartOf")
    vpTable20.createOrReplaceTempView("ExtVP_SS_SeeAlso_PartOf")
    vpTable21.createOrReplaceTempView("ExtVP_SS_Pages_PartOf")
    vpTable22.createOrReplaceTempView("ExtVP_SS_Title_PartOf")
    vpTable23.createOrReplaceTempView("ExtVP_SS_Type_Creator")
    vpTable24.createOrReplaceTempView("VP_SubClassOf")
    vpTable25.createOrReplaceTempView("ExtVP_SS_Issued_Creator")
    vpTable26.createOrReplaceTempView("ExtVP_SS_Creator_Issued")

    vpTable28.createOrReplaceTempView("VP_SeeAlso")
    vpTable29.createOrReplaceTempView("VP_Editor")
    vpTable30.createOrReplaceTempView("ExtVP_SS_Type_Name")
    /*
    */

    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/csv/ExtVP/$ds.txt"), true)

    val queries = List(
      new ExtVPQueries q1,
      new ExtVPQueries q2,
      new ExtVPQueries q3,
      new ExtVPQueries q4,
      new ExtVPQueries q5,
      new ExtVPQueries q6,
      new ExtVPQueries q8,
      new ExtVPQueries q9,
      new ExtVPQueries q10,
      new ExtVPQueries q11,
    )

    println("Running queries")

    var count = 1
    for (query <- queries) {
      //run query and calculate the run time
      val starttime = System.nanoTime()
      val df = spark.sql(query)
      df.take(100).foreach(println)
      val endtime = System.nanoTime()
      val result = (endtime - starttime).toDouble / 1000000000

      //write the result into the log file
      if (count != queries.size) {
        Console.withOut(fos) {
          print(result + ",")
        }
      } else {
        Console.withOut(fos) {
          println(result)
        }
      }
      count += 1
    }
    println("All Queries are Done - CSV - ExtVP!")

  }
}
