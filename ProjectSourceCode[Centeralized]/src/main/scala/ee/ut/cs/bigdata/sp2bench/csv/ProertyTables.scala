package ee.ut.cs.bigdata.sp2bench.csv


import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import ee.ut.cs.bigdata.sp2bench.queries.PTQueries

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
      .getOrCreate()



    val ds="10M"
    val path=s"hdfs://quickstart:8020/user/cloudera/RDFBench/SP2B/$ds/CSV/PropertyTables"


    val RDFDFDocument = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Document.csv").toDF()
    RDFDFDocument.createOrReplaceTempView("Document")
    RDFDFDocument.persist()

    val RDFDFVenue= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Venue.csv").toDF()
    RDFDFVenue.createOrReplaceTempView("Venue")

    val RDFDFPerson = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Person.csv").toDF()
    RDFDFPerson.createOrReplaceTempView("Person")

    val RDFDFPublication= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Publication.csv").toDF()
    RDFDFPublication.createOrReplaceTempView("Publication")
    RDFDFPublication.persist()

    val RDFDFVenueType = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VenueType.csv").toDF()
    RDFDFVenueType.createOrReplaceTempView("VenueType")

    val RDFDFPublicationType = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/PublicationType.csv").toDF()
    RDFDFPublicationType.createOrReplaceTempView("PublicationType")

    val RDFDFAuthor = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Author.csv").toDF()
    RDFDFAuthor.createOrReplaceTempView("Author")


    val RDFDFEditor = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Editor.csv").toDF()
    RDFDFEditor.createOrReplaceTempView("Editor")


    val RDFDFDocumentSeeAlso = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Document_seeAlso.csv").toDF()
    RDFDFDocumentSeeAlso.createOrReplaceTempView("Document_seeAlso")

    val RDFDFDocumentHomepage = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Document_homepage.csv").toDF()
    RDFDFDocumentHomepage.createOrReplaceTempView("Document_homepage")


    val RDFDFDocumentAbstract = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Abstract.csv").toDF()
    RDFDFDocumentAbstract.createOrReplaceTempView("Abstract")


    val RDFDFReference = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/Reference.csv").toDF()
    RDFDFReference.createOrReplaceTempView("Reference")
    RDFDFReference.persist()


    val fos = new FileOutputStream(new File(s"/home/cloudera/Downloads/Results/csv/PT/$ds.txt"),true)



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
