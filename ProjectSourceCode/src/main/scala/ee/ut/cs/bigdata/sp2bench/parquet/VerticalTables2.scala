package ee.ut.cs.bigdata.sp2bench.parquet

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
      .appName("RDFBench Parquet VT")
      .getOrCreate()

    import spark.implicits._
    val ds=args(0)			 //value = {"100M", "500M, or "1B"} 
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds"

    //read tables from HDFS

    val RDFDFType = spark.read.format("parquet").load(s"$path/VP/Parquet/type.parquet").toDF()
    RDFDFType.createOrReplaceTempView("type")

    val RDFDFpredicatescombined= spark.read.format("parquet").load(s"$path/ST/Parquet/SingleStmtTable.parquet").toDF()
    RDFDFpredicatescombined.createOrReplaceTempView("SingleStmtTable")


/*
    val RDFDFTitle = spark.read.format("parquet").load(s"$path/VP/Parquet/title.parquet").toDF()
    RDFDFTitle.createOrReplaceTempView("title")

    val RDFDFIssued = spark.read.format("parquet").load(s"$path/VP/Parquet/issued.parquet").toDF()
    RDFDFIssued.createOrReplaceTempView("issued")


    val RDFDFCreator = spark.read.format("parquet").load(s"$path/VP/Parquet/creator.parquet").toDF()
    RDFDFCreator.createOrReplaceTempView("creator")

    val RDFDFBookTitle = spark.read.format("parquet").load(s"$path/VP/Parquet/booktitle.parquet").toDF()
    RDFDFBookTitle.createOrReplaceTempView("booktitle")

    val RDFDFPartOf = spark.read.format("parquet").load(s"$path/VP/Parquet/partOf.parquet").toDF()
    RDFDFPartOf.createOrReplaceTempView("partOf")

    val RDFDFSeeAlso = spark.read.format("parquet").load(s"$path/VP/Parquet/seeAlso.parquet").toDF()
    RDFDFSeeAlso.createOrReplaceTempView("seeAlso")

    val RDFDFPages= spark.read.format("parquet").load(s"$path/VP/Parquet/pages.parquet").toDF()
    RDFDFPages.createOrReplaceTempView("pages")

    val RDFDFHomePage= spark.read.format("parquet").load(s"$path/VP/Parquet/homepage.parquet").toDF()
    RDFDFHomePage.createOrReplaceTempView("homePage")

    val RDFDFAbstract= spark.read.format("parquet").load(s"$path/VP/Parquet/abstract.parquet").toDF()
    RDFDFAbstract.createOrReplaceTempView("abstractv")

    val RDFDFName= spark.read.format("parquet").load(s"$path/VP/Parquet/name.parquet").toDF()
    RDFDFName.createOrReplaceTempView("name")

    val RDFDFJournal= spark.read.format("parquet").load(s"$path/VP/Parquet/journal.parquet").toDF()
    RDFDFJournal.createOrReplaceTempView("journal")

    val RDFDFSubClassOf= spark.read.format("parquet").load(s"$path/VP/Parquet/subClassOf.parquet").toDF()
    RDFDFSubClassOf.createOrReplaceTempView("subClassOf")

    val RDFDFReferencesV= spark.read.format("parquet").load(s"$path/VP/Parquet/references.parquet").toDF()
    RDFDFReferencesV.createOrReplaceTempView("referencesv")

    val RDFDFReferences= spark.read.format("parquet").load(s"$path/PT/Parquet/Reference.parquet").toDF()
    RDFDFReferences.createOrReplaceTempView("reference")

    val RDFDFEditor= spark.read.format("parquet").load(s"$path/VP/Parquet/editor.parquet").toDF()
    RDFDFEditor.createOrReplaceTempView("editorv")
*/

    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/parquet/VP/$ds.txt"),true)

    val queries = List(new VTQueries q9)
		      /* new VTQueries q10) 
		       new VTQueries q3,
		       new VTQueries q4,
		       new VTQueries q5,
		       new VTQueries q6,
		       new VTQueries q7,
		       new VTQueries q8,
		       new VTQueries q9,
		       new VTQueries q10,
		       new VTQueries q11)*/
        
	//val queries = List(new VTQueries q4)


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
    println("All Queries are Done - PARQUET - VP!")

  }
}

