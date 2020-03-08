package ee.ut.cs.bigdata.sp2bench.postgresql

import java.io.{File, FileOutputStream}

import ee.ut.cs.bigdata.sp2bench.queries.VTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object VerticalTables {


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
    
    
    val pgsqlDB="rdfbench10m"

    val RDFDFTitle = spark.read
      .format("jdbc")
     .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "title")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFTitle.createOrReplaceTempView("Title")


    val RDFDFIssued = spark.read
      .format("jdbc")
     .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "issued")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFIssued.createOrReplaceTempView("Issued")


    val RDFDFType = spark.read
      .format("jdbc")
     .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "type")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()
    RDFDFType.createOrReplaceTempView("Type")


    val RDFDFCreator = spark.read
      .format("jdbc")
     .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "creator")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()
    RDFDFCreator.createOrReplaceTempView("Creator")


    val RDFDFBookTitle = spark.read
      .format("jdbc")
     .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "booktitle")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()
    RDFDFBookTitle.createOrReplaceTempView("BookTitle")

    val RDFDFPartOf = spark.read
      .format("jdbc")
     .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "partof")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()
    RDFDFPartOf.createOrReplaceTempView("PartOf")


    val RDFDFSeeAlso = spark.read
      .format("jdbc")
     .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "seealso")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()
    RDFDFSeeAlso.createOrReplaceTempView("SeeAlso")

    val RDFDFPages = spark.read
      .format("jdbc")
     .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "pages")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()
    RDFDFPages.createOrReplaceTempView("Pages")


    val RDFDFHomePage = spark.read
      .format("jdbc")
     .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "homepage")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()
    RDFDFHomePage.createOrReplaceTempView("HomePage")


    val RDFDFAbstract = spark.read
      .format("jdbc")
     .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "abstractv")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()
    RDFDFAbstract.createOrReplaceTempView("AbstractV")


    val RDFDFName = spark.read
      .format("jdbc")
     .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "name")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()
    RDFDFName.createOrReplaceTempView("Name")


    val RDFDFJournal = spark.read
      .format("jdbc")
     .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "journal")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()
    RDFDFJournal.createOrReplaceTempView("Journal")



    val RDFDFSubClassOf = spark.read
      .format("jdbc")
     .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "subclassof")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()
    RDFDFSubClassOf.createOrReplaceTempView("subclassof")



    val RDFDFEditor = spark.read
      .format("jdbc")
     .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "editorv")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()
    RDFDFEditor.createOrReplaceTempView("EditorV")



    val RDFDFRefrencesV = spark.read
      .format("jdbc")
     .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "Referencesv")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFRefrencesV.createOrReplaceTempView("Referencesv")



    val RDFDFRefrences = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "Reference")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    RDFDFRefrences.createOrReplaceTempView("Reference")



    val SingleTableDF = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/$pgsqlDB")
      .option("dbtable", "singlestmttable")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    SingleTableDF.createOrReplaceTempView("SingleStmtTable")



    val fos = new FileOutputStream(new File(s"/home/cloudera/Downloads/Results/postgres/VT/$pgsqlDB.txt"),true)


    Console.withOut(fos) {"Q1:"+spark.time(spark.sql(new VTQueries q1).show)}
    Console.withOut(fos) {"Q2:"+spark.time(spark.sql(new VTQueries q2).show)}
    Console.withOut(fos) {"Q3:"+spark.time(spark.sql(new VTQueries q3).show)}
    Console.withOut(fos) {"Q4:"+spark.time(spark.sql(new VTQueries q4).show)}
    Console.withOut(fos) {"Q5:"+spark.time(spark.sql(new VTQueries q5).show)}
    Console.withOut(fos) {"Q6:"+spark.time(spark.sql(new VTQueries q6).show)}
    Console.withOut(fos) {"Q8:"+spark.time(spark.sql(new VTQueries q8).show)}
    Console.withOut(fos) {"Q9:"+spark.time(spark.sql(new VTQueries q9).show)}
    Console.withOut(fos) {"Q10:"+spark.time(spark.sql(new VTQueries q10).show)}
    Console.withOut(fos) {"Q11:"+spark.time(spark.sql(new VTQueries q11).show)}

    Console.withOut(fos) {"Q7:"+spark.time(spark.sql(new VTQueries q7).show())}

    Console.withOut(fos) {println("=======================================")}
    println("All Queries are Done!")



  }
}