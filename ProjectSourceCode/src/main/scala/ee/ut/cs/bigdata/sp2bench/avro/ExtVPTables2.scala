package ee.ut.cs.bigdata.sp2bench.avro

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.ExtVPQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ExtVPTables2 {
  def main(args: Array[String]): Unit = {
    println("ExtVP VHDFS Avro")

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Avro ExtVP")
      .getOrCreate()

    println("Spark session is created!")

    import spark.implicits._
    val ds = args(0) // value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds"

    println("Reading Tables...")
    //read tables from HDFS

    val vpTable6 = spark.read.format("avro").load(s"$path/VP/Avro/type.avro").toDF()
    val vpTable27 = spark.read.format("avro").load(s"$path/ST/Avro/SingleStmtTable.avro").toDF()

    val vpTable1 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/type/issued.avro").toDF()
    val vpTable2 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/title/issued.avro").toDF()
    val vpTable3 = spark.read.format("avro").load(s"$path/VP/Avro/issued.avro").toDF()
    val vpTable4 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/type/pages.avro").toDF()
    val vpTable5 = spark.read.format("avro").load(s"$path/VP/Avro/pages.avro").toDF()

    val vpTable7 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SO/name/creator.avro").toDF()
    val vpTable8 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/creator/journal.avro").toDF()
    val vpTable9 = spark.read.format("avro").load(s"$path/VP/Avro/journal.avro").toDF()
    val vpTable10 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/type/journal.avro").toDF()
    val vpTable11 = spark.read.format("avro").load(s"$path/VP/Avro/name.avro").toDF()
    val vpTable12 = spark.read.format("avro").load(s"$path/VP/Avro/creator.avro").toDF()
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

    val vpTable28 = spark.read.format("avro").load(s"$path/VP/Avro/seeAlso.avro").toDF()
    val vpTable29 = spark.read.format("avro").load(s"$path/VP/Avro/editor.avro").toDF()
    val vpTable30 = spark.read.format("avro").load(s"$path/ExtVP/Avro/SS/type/name.avro").toDF()
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
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/avro/ExtVP/$ds.txt"), true)
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
    println("All Queries are Done - Avro - ExtVP!")

  }
}
