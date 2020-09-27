package ee.ut.cs.bigdata.sp2bench.parquet

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
      .appName("RDFBench Parquet VT")
      .getOrCreate()

    import spark.implicits._
    val ds=args(0)			 //value = {"100M", "500M, or "1B"} 
    val partitionType = args(1)		 //value = {"Horizontal", "Subject", or "Predicate"}   
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds/Parquet"

    //read tables from HDFS
    if(partitionType.toLowerCase == "predicate")
    {
    val RDFDFTitle = spark.read.format("parquet").load(s"$path/VT/title.parquet").toDF()
    RDFDFTitle.createOrReplaceTempView("title")

    val RDFDFIssued = spark.read.format("parquet").load(s"$path/VT/issued.parquet").toDF()
    RDFDFIssued.createOrReplaceTempView("issued")

    val RDFDFType = spark.read.format("parquet").load(s"$path/VT/type.parquet").toDF()
    RDFDFType.createOrReplaceTempView("type")

    val RDFDFCreator = spark.read.format("parquet").load(s"$path/VT/creator.parquet").toDF()
    RDFDFCreator.createOrReplaceTempView("creator")

    val RDFDFBookTitle = spark.read.format("parquet").load(s"$path/VT/booktitle.parquet").toDF()
    RDFDFBookTitle.createOrReplaceTempView("booktitle")

    val RDFDFPartOf = spark.read.format("parquet").load(s"$path/VT/partof.parquet").toDF()
    RDFDFPartOf.createOrReplaceTempView("partOf")

    val RDFDFSeeAlso = spark.read.format("parquet").load(s"$path/VT/seealso.parquet").toDF()
    RDFDFSeeAlso.createOrReplaceTempView("seeAlso")

    val RDFDFPages= spark.read.format("parquet").load(s"$path/VT/pages.parquet").toDF()
    RDFDFPages.createOrReplaceTempView("pages")

    val RDFDFHomePage= spark.read.format("parquet").load(s"$path/VT/homepage.parquet").toDF()
    RDFDFHomePage.createOrReplaceTempView("homePage")

    val RDFDFAbstract= spark.read.format("parquet").load(s"$path/VT/abstract.parquet").toDF()
    RDFDFAbstract.createOrReplaceTempView("abstractv")

    val RDFDFName= spark.read.format("parquet").load(s"$path/VT/name.parquet").toDF()
    RDFDFName.createOrReplaceTempView("name")

    val RDFDFJournal= spark.read.format("parquet").load(s"$path/VT/injournal.parquet").toDF()
    RDFDFJournal.createOrReplaceTempView("journal")

    val RDFDFSubClassOf= spark.read.format("parquet").load(s"$path/VT/subclassof.parquet").toDF()
    RDFDFSubClassOf.createOrReplaceTempView("subClassOf")

    val RDFDFReferencesV= spark.read.format("parquet").load(s"$path/VT/references.parquet").toDF()
    RDFDFReferencesV.createOrReplaceTempView("referencesv")

    val RDFDFReferences= spark.read.format("parquet").load(s"$path/PT/Reference.parquet").toDF()
    RDFDFReferences.createOrReplaceTempView("reference")

    val RDFDFEditor= spark.read.format("parquet").load(s"$path/VT/editor.parquet").toDF()
    RDFDFEditor.createOrReplaceTempView("editorv")

    val RDFDFpredicatescombined= spark.read.format("parquet").load(s"$path/ST/SingleStmtTable").toDF()
    RDFDFpredicatescombined.createOrReplaceTempView("SingleStmtTable")
    }
 
    else
    {

    val RDFDFTitle = spark.read.format("parquet").load(s"$path/VT/title$partitionType.parquet").toDF()
    RDFDFTitle.createOrReplaceTempView("title")

    val RDFDFIssued = spark.read.format("parquet").load(s"$path/VT/issued$partitionType.parquet").toDF()
    RDFDFIssued.createOrReplaceTempView("issued")

    val RDFDFType = spark.read.format("parquet").load(s"$path/VT/type$partitionType.parquet").toDF()
    RDFDFType.createOrReplaceTempView("type")

    val RDFDFCreator = spark.read.format("parquet").load(s"$path/VT/creator$partitionType.parquet").toDF()
    RDFDFCreator.createOrReplaceTempView("creator")

    val RDFDFBookTitle = spark.read.format("parquet").load(s"$path/VT/booktitle$partitionType.parquet").toDF()
    RDFDFBookTitle.createOrReplaceTempView("booktitle")

    val RDFDFPartOf = spark.read.format("parquet").load(s"$path/VT/partof$partitionType.parquet").toDF()
    RDFDFPartOf.createOrReplaceTempView("partOf")

    val RDFDFSeeAlso = spark.read.format("parquet").load(s"$path/VT/seealso$partitionType.parquet").toDF()
    RDFDFSeeAlso.createOrReplaceTempView("seeAlso")

    val RDFDFPages= spark.read.format("parquet").load(s"$path/VT/pages$partitionType.parquet").toDF()
    RDFDFPages.createOrReplaceTempView("pages")

    val RDFDFHomePage= spark.read.format("parquet").load(s"$path/VT/homepage$partitionType.parquet").toDF()
    RDFDFHomePage.createOrReplaceTempView("homePage")

    val RDFDFAbstract= spark.read.format("parquet").load(s"$path/VT/abstract$partitionType.parquet").toDF()
    RDFDFAbstract.createOrReplaceTempView("abstractv")

    val RDFDFName= spark.read.format("parquet").load(s"$path/VT/name$partitionType.parquet").toDF()
    RDFDFName.createOrReplaceTempView("name")

    val RDFDFJournal= spark.read.format("parquet").load(s"$path/VT/injournal$partitionType.parquet").toDF()
    RDFDFJournal.createOrReplaceTempView("journal")

    val RDFDFSubClassOf= spark.read.format("parquet").load(s"$path/VT/subclassof$partitionType.parquet").toDF()
    RDFDFSubClassOf.createOrReplaceTempView("subClassOf")

    val RDFDFReferencesV= spark.read.format("parquet").load(s"$path/VT/references$partitionType.parquet").toDF()
    RDFDFReferencesV.createOrReplaceTempView("referencesv")

    val RDFDFReferences= spark.read.format("parquet").load(s"$path/PT/Reference$partitionType.parquet").toDF()
    RDFDFReferences.createOrReplaceTempView("reference")

    val RDFDFEditor= spark.read.format("parquet").load(s"$path/VT/editor$partitionType.parquet").toDF()
    RDFDFEditor.createOrReplaceTempView("editorv")

    val RDFDFpredicatescombined= spark.read.format("parquet").load(s"$path/ST/SingleStmtTable$partitionType").toDF()
    RDFDFpredicatescombined.createOrReplaceTempView("SingleStmtTable")
    }


    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/parquet/VT/$ds$partitionType.txt"),true)

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
    println("All Queries are Done - PARQUET - VT!")

  }
}

