package ee.ut.cs.bigdata.sp2bench.orc

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
      .appName("RDFBench ORC VT")
      .getOrCreate()

    import spark.implicits._
    val ds=args(0)			//value = {"100M", "500M, or "1B"} 
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds"

    //read tables from HDFS
     val RDFDFType = spark.read.format("orc").load(s"$path/VP/ORC/type.orc").toDF()
    RDFDFType.createOrReplaceTempView("type")

    val RDFDFPredicatescombined= spark.read.format("orc").load(s"$path/ST/ORC/SingleStmtTable.orc").toDF()
    RDFDFPredicatescombined.createOrReplaceTempView("SingleStmtTable")

/*
    val RDFDFTitle = spark.read.format("orc").load(s"$path/VP/ORC/title.orc").toDF()
    RDFDFTitle.createOrReplaceTempView("title")

    val RDFDFIssued = spark.read.format("orc").load(s"$path/VP/ORC/issued.orc").toDF()
    RDFDFIssued.createOrReplaceTempView("issued")


    val RDFDFCreator = spark.read.format("orc").load(s"$path/VP/ORC/creator.orc").toDF()
    RDFDFCreator.createOrReplaceTempView("creator")

    val RDFDFBookTitle = spark.read.format("orc").load(s"$path/VP/ORC/booktitle.orc").toDF()
    RDFDFBookTitle.createOrReplaceTempView("booktitle")

    val RDFDFPartOf = spark.read.format("orc").load(s"$path/VP/ORC/partOf.orc").toDF()
    RDFDFPartOf.createOrReplaceTempView("partOf")

    val RDFDFSeeAlso = spark.read.format("orc").load(s"$path/VP/ORC/seeAlso.orc").toDF()
    RDFDFSeeAlso.createOrReplaceTempView("seeAlso")

    val RDFDFPages= spark.read.format("orc").load(s"$path/VP/ORC/pages.orc").toDF()
    RDFDFPages.createOrReplaceTempView("pages")

    val RDFDFHomePage= spark.read.format("orc").load(s"$path/VP/ORC/homepage.orc").toDF()
    RDFDFHomePage.createOrReplaceTempView("homePage")

    val RDFDFAbstract= spark.read.format("orc").load(s"$path/VP/ORC/abstract.orc").toDF()
    RDFDFAbstract.createOrReplaceTempView("abstractv")

    val RDFDFName= spark.read.format("orc").load(s"$path/VP/ORC/name.orc").toDF()
    RDFDFName.createOrReplaceTempView("name")

    val RDFDFJournal= spark.read.format("orc").load(s"$path/VP/ORC/journal.orc").toDF()
    RDFDFJournal.createOrReplaceTempView("journal")

    val RDFDFSubClassOf= spark.read.format("orc").load(s"$path/VP/ORC/subClassOf.orc").toDF()
    RDFDFSubClassOf.createOrReplaceTempView("subClassOf")

    val RDFDFReferencesV= spark.read.format("orc").load(s"$path/VP/ORC/references.orc").toDF()
    RDFDFReferencesV.createOrReplaceTempView("referencesv")

    val RDFDFReferences= spark.read.format("orc").load(s"$path/PT/ORC/Reference.orc").toDF()
    RDFDFReferences.createOrReplaceTempView("reference")

    val RDFDFEditor= spark.read.format("orc").load(s"$path/VP/ORC/editor.orc").toDF()
    RDFDFEditor.createOrReplaceTempView("editorv")
*/

    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/orc/VP/$ds.txt"),true) 

    val queries = List(new VTQueries q9)
		     /*  new VTQueries q10) ,
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
    println("All Queries are Done - ORC - VT!")

  }
}
