package ee.ut.cs.bigdata.sp2bench.csv

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.VTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object VerticalTables {
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
    val ds=args(0)			// value = {"100M", "500M, or "1B"} 
    val partitionType = args(1)		// value = {"Horizontal", "Subject", or "Predicate"}
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/CSV"

    //read tables from HDFS
    if(partitionType.toLowerCase == "predicate")
    {
    val RDFDFTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/title.csv").toDF()
    RDFDFTitle.createOrReplaceTempView("title")

    val RDFDFIssued = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/issued.csv").toDF()
    RDFDFIssued.createOrReplaceTempView("issued")

    val RDFDFType = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/type.csv").toDF()
    RDFDFType.createOrReplaceTempView("type")

    val RDFDFCreator = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/creator.csv").toDF()
    RDFDFCreator.createOrReplaceTempView("creator")

    val RDFDFBookTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/booktitle.csv").toDF()
    RDFDFBookTitle.createOrReplaceTempView("booktitle")

    val RDFDFPartOf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/partof.csv").toDF()
    RDFDFPartOf.createOrReplaceTempView("partOf")

    val RDFDFSeeAlso = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/seealso.csv").toDF()
    RDFDFSeeAlso.createOrReplaceTempView("seeAlso")

    val RDFDFPages= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/pages.csv").toDF()
    RDFDFPages.createOrReplaceTempView("pages")

    val RDFDFHomePage= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/homepage.csv").toDF()
    RDFDFHomePage.createOrReplaceTempView("homePage")

    val RDFDFAbstract= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/abstract.csv").toDF()
    RDFDFAbstract.createOrReplaceTempView("abstractv")

    val RDFDFName= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/name.csv").toDF()
    RDFDFName.createOrReplaceTempView("name")

    val RDFDFJournal= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/injournal.csv").toDF()
    RDFDFJournal.createOrReplaceTempView("journal")

    val RDFDFSubClassOf= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/subclassof.csv").toDF()
    RDFDFSubClassOf.createOrReplaceTempView("subClassOf")

    val RDFDFReferencesV= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/references.csv").toDF()
    RDFDFReferencesV.createOrReplaceTempView("referencesv")

    val RDFDFReferences= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/PT/Reference.csv").toDF()
    RDFDFReferences.createOrReplaceTempView("reference")

    val RDFDFEditor= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/editor.csv").toDF()
    RDFDFEditor.createOrReplaceTempView("editorv")

    val RDFDFPredicatescombined= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ST/SingleStmtTable.csv").toDF()
    RDFDFPredicatescombined.createOrReplaceTempView("SingleStmtTable")
    }

    else 
    {

    val RDFDFTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/title$partitionType.csv").toDF()
    RDFDFTitle.createOrReplaceTempView("title")

    val RDFDFIssued = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/issued$partitionType.csv").toDF()
    RDFDFIssued.createOrReplaceTempView("issued")

    val RDFDFType = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/type$partitionType.csv").toDF()
    RDFDFType.createOrReplaceTempView("type")

    val RDFDFCreator = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/creator$partitionType.csv").toDF()
    RDFDFCreator.createOrReplaceTempView("creator")

    val RDFDFBookTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/booktitle$partitionType.csv").toDF()
    RDFDFBookTitle.createOrReplaceTempView("booktitle")

    val RDFDFPartOf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/partof$partitionType.csv").toDF()
    RDFDFPartOf.createOrReplaceTempView("partOf")

    val RDFDFSeeAlso = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/seealso$partitionType.csv").toDF()
    RDFDFSeeAlso.createOrReplaceTempView("seeAlso")

    val RDFDFPages= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/pages$partitionType.csv").toDF()
    RDFDFPages.createOrReplaceTempView("pages")

    val RDFDFHomePage= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/homepage$partitionType.csv").toDF()
    RDFDFHomePage.createOrReplaceTempView("homePage")

    val RDFDFAbstract= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/abstract$partitionType.csv").toDF()
    RDFDFAbstract.createOrReplaceTempView("abstractv")

    val RDFDFName= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/name$partitionType.csv").toDF()
    RDFDFName.createOrReplaceTempView("name")

    val RDFDFJournal= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/injournal$partitionType.csv").toDF()
    RDFDFJournal.createOrReplaceTempView("journal")

    val RDFDFSubClassOf= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/subclassof$partitionType.csv").toDF()
    RDFDFSubClassOf.createOrReplaceTempView("subClassOf")

    val RDFDFReferencesV= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/references$partitionType.csv").toDF()
    RDFDFReferencesV.createOrReplaceTempView("referencesv")

    val RDFDFReferences= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/PT/Reference$partitionType.csv").toDF()
    RDFDFReferences.createOrReplaceTempView("reference")

    val RDFDFEditor= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/VT/editor$partitionType.csv").toDF()
    RDFDFEditor.createOrReplaceTempView("editorv")

    val RDFDFPredicatescombined= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ST/SingleStmtTable$partitionType.csv").toDF()
    RDFDFPredicatescombined.createOrReplaceTempView("SingleStmtTable")
    }

    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/csv/VT/$ds$partitionType.txt"),true)

    val queries = List(new VTQueries q1,
		       new VTQueries q2,
		       new VTQueries q3,
		       new VTQueries q4,
		       new VTQueries q5,
		       new VTQueries q6,
		       new VTQueries q7,
		       new VTQueries q8,
		       new VTQueries q9,
		       new VTQueries q10,
		       new VTQueries q11)

    var count = 1
    for (query <- queries)
    { 
       //run query and calculate the run time
       val starttime=System.nanoTime()
       val df=spark.sql(query)
       df.take(100).foreach(println)
       val endtime=System.nanoTime()
       val result = (endtime-starttime).toDouble/1000000000

       //write the result into the log file
       if( count != queries.size ) {
           Console.withOut(fos){print(result + ",")}
       } else {
           Console.withOut(fos){println(result)}
       }
       count+=1   
     }  
    println("All Queries are Done - CSV - VT!") 

  }
}
