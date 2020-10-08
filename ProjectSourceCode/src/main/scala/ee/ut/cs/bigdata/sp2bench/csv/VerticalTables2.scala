package ee.ut.cs.bigdata.sp2bench.csv

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.VTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object VerticalTables2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench CSV VT")
      .getOrCreate()

    import spark.implicits._
    val ds = args(0) // value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds"

    //read tables from HDFS

    val RDFDFType = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/type.csv").toDF()
    RDFDFType.createOrReplaceTempView("type")

    val RDFDFPredicatescombined = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ST/CSV/SingleStmtTable.csv").toDF()
    RDFDFPredicatescombined.createOrReplaceTempView("SingleStmtTable")


    /*

        val RDFDFTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/title.csv").toDF()
        RDFDFTitle.createOrReplaceTempView("title")

        val RDFDFIssued = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/issued.csv").toDF()
        RDFDFIssued.createOrReplaceTempView("issued")


        val RDFDFCreator = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/creator.csv").toDF()
        RDFDFCreator.createOrReplaceTempView("creator")

        val RDFDFBookTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/booktitle.csv").toDF()
        RDFDFBookTitle.createOrReplaceTempView("booktitle")

        val RDFDFPartOf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/partOf.csv").toDF()
        RDFDFPartOf.createOrReplaceTempView("partOf")

        val RDFDFSeeAlso = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/seeAlso.csv").toDF()
        RDFDFSeeAlso.createOrReplaceTempView("seeAlso")

        val RDFDFPages= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/pages.csv").toDF()
        RDFDFPages.createOrReplaceTempView("pages")

        val RDFDFHomePage= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/homepage.csv").toDF()
        RDFDFHomePage.createOrReplaceTempView("homePage")

        val RDFDFAbstract= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/abstract.csv").toDF()
        RDFDFAbstract.createOrReplaceTempView("abstractv")

        val RDFDFName= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/name.csv").toDF()
        RDFDFName.createOrReplaceTempView("name")

        val RDFDFJournal= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/journal.csv").toDF()
        RDFDFJournal.createOrReplaceTempView("journal")

        val RDFDFSubClassOf= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/subClassOf.csv").toDF()
        RDFDFSubClassOf.createOrReplaceTempView("subClassOf")

        val RDFDFReferencesV= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/references.csv").toDF()
        RDFDFReferencesV.createOrReplaceTempView("referencesv")

        val RDFDFReferences= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/PT/CSV/Reference.csv").toDF()
        RDFDFReferences.createOrReplaceTempView("reference")

        val RDFDFEditor= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VP/CSV/editor.csv").toDF()
        RDFDFEditor.createOrReplaceTempView("editorv")
    */

    /*testing
    println("type")
    spark.sql(" select count (*) from type").show()

    println("creator")
    spark.sql(" select count (*) from creator").show()

    println("booktitle")
    spark.sql(" select count (*) from booktitle").show()

    println("title")
    spark.sql(" select count (*) from title").show()

    println("partof")
    spark.sql(" select count (*) from partOf").show()

    println("seealso")
    spark.sql(" select count (*) from seeAlso").show()

    println("pages")
    spark.sql(" select count (*) from pages").show()

    println("homepage")
    spark.sql(" select count (*) from homepage").show()

    println("issued")
    spark.sql(" select count (*) from issued").show()

    println("abstractv")
    spark.sql(" select count (*) from abstractv").show()
    */


    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/csv/VP/$ds.txt"), true)

    val queries = List(new VTQueries q9)
    /*new VTQueries q10)
    new VTQueries q3,
    new VTQueries q4,
    new VTQueries q5,
    new VTQueries q6,
    new VTQueries q7,
    new VTQueries q8,
    new VTQueries q9,
    new VTQueries q10,
    new VTQueries q11) */

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
    println("All Queries are Done - CSV - VP!")

  }
}
