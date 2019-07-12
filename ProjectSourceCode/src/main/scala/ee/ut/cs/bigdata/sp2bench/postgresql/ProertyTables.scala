package ee.ut.cs.bigdata.sp2bench.postgresql

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
     //.config("spark.sql.broadcastTimeout", "36000")
      .getOrCreate()




    val pgsqlDB="rdfbench10m"



    val RDFDFvenue= spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "venue")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFvenue.createOrReplaceTempView("Venue")



    val RDFDFVenueType= spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "venuetype")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFVenueType.createOrReplaceTempView("VenueType")


    val RDFDFDocument = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "Document")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFDocument.createOrReplaceTempView("Document")



    val RDFDFDocumentSeeAlso = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "document_seealso")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFDocumentSeeAlso.createOrReplaceTempView("Document_seealso")


    val RDFDFDocumenthomepage = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "document_homepage")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFDocumenthomepage.createOrReplaceTempView("Document_homepage")



    val RDFDFAbstract= spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "Abstract")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFAbstract.createOrReplaceTempView("Abstract")

    val RDFDFPublication = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "Publication")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFPublication.createOrReplaceTempView("Publication")



    val RDFDFReference = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "Reference")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFReference.createOrReplaceTempView("Reference")


    val RDFDFPublication_cdrom= spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "Publication_cdrom")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFPublication_cdrom.createOrReplaceTempView("Publication_cdrom")



    val RDFDFPublicationType = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "PublicationType")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFPublicationType.createOrReplaceTempView("PublicationType")




    val RDFDFPerson = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "person")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFPerson.createOrReplaceTempView("Person")




    val RDFDFAuthor= spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "Author")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFAuthor.createOrReplaceTempView("Author")




    val RDFDFEditor= spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "Editor")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFEditor.createOrReplaceTempView("Editor")


    val fos = new FileOutputStream(new File(s"/home/cloudera/Downloads/Results/postgres/PT/$pgsqlDB.txt"),true)


    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q1).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q2).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q3).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q4).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q5).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q6).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q8).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q10).show) }
    Console.withOut(fos) {spark.time(spark.sql(new PTQueries q11).show) }

   /* try{Console.withOut(fos) {spark.time(spark.sql(new PTQueries q7).show())}}
    catch {case toe: TimeoutException=>Console.withOut(fos) {println("Time taken:  ms")}}*/

    Console.withOut(fos) {println("===================================")}
    println("All Queries are Done!")


  }

}
