package ee.ut.cs.bigdata.sp2bench.parquet


import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException

import ee.ut.cs.bigdata.sp2bench.queries.PTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ProertyTables {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local").setAppName("SQLSPARK")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")



    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SparkSessionZipsExample")
      //.config("spark.sql.broadcastTimeout",1200)
      .getOrCreate()




    val ds="10M"
    val path=s"hdfs://quickstart:8020/user/cloudera/RDFBench/SP2B/$ds/Parquet/PropertyTables"


    val RDFDFDocument = spark.read.format("parquet").load(s"$path/Document.parquet").toDF()
    RDFDFDocument.createOrReplaceTempView("Document")

    val RDFDFVenue= spark.read.format("parquet").load(s"$path/Venue.parquet").toDF()
    RDFDFVenue.createOrReplaceTempView("Venue")

    val RDFDFPerson = spark.read.format("parquet").load(s"$path/Person.parquet").toDF()
    RDFDFPerson.createOrReplaceTempView("Person")

    val RDFDFPublication= spark.read.format("parquet").load(s"$path/Publication.parquet").toDF()
    RDFDFPublication.createOrReplaceTempView("Publication")

    val RDFDFVenueType = spark.read.format("parquet").load(s"$path/VenueType.parquet").toDF()
    RDFDFVenueType.createOrReplaceTempView("VenueType")

    val RDFDFPublicationType = spark.read.format("parquet").load(s"$path/PublicationType.parquet").toDF()
    RDFDFPublicationType.createOrReplaceTempView("PublicationType")

    val RDFDFAuthor = spark.read.format("parquet").load(s"$path/Author.parquet").toDF()
    RDFDFAuthor.createOrReplaceTempView("Author")


    val RDFDFEditor = spark.read.format("parquet").load(s"$path/Editor.parquet").toDF()
    RDFDFEditor.createOrReplaceTempView("Editor")


    val RDFDFDocumentSeeAlso = spark.read.format("parquet").load(s"$path/Document_seeAlso.parquet").toDF()
    RDFDFDocumentSeeAlso.createOrReplaceTempView("Document_seeAlso")

    val RDFDFDocumentHomepage = spark.read.format("parquet").load(s"$path/Document_homepage.parquet").toDF()
    RDFDFDocumentHomepage.createOrReplaceTempView("Document_homepage")


    val RDFDFDocumentAbstract = spark.read.format("parquet").load(s"$path/Abstract.parquet").toDF()
    RDFDFDocumentAbstract.createOrReplaceTempView("Abstract")


    val RDFDFReference = spark.read.format("parquet").load(s"$path/Reference.parquet").toDF()
    RDFDFReference.createOrReplaceTempView("Reference")


    val fos = new FileOutputStream(new File(s"/home/cloudera/Downloads/Results/parquet/PT/$ds.txt"),true)



    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q1).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q2).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q3).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q4).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q5).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q6).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q8).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q10).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q11).show) }

    try{Console.withOut(fos) {spark.time(spark.sql(new PTQueries q7).show())}}
    catch {case toe: TimeoutException=>Console.withOut(fos) {println("Time taken:  ms")}}

    Console.withOut(fos) {println("===================================")}
    println("All Queries are Done!")
  }

}
