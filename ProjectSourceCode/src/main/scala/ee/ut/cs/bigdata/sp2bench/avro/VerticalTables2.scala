
package ee.ut.cs.bigdata.sp2bench.avro

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
      .appName("RDFBench Avro VT")
      .getOrCreate()
    
    import spark.implicits._
    val ds=args(0)			// value = {"100M", "500M, or "1B"}  
    val partitionType = args(1)		// value = {"Horizontal", "Subject", or "Predicate"}
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds"

    //read splitted Document tables

    val RDFDFType = spark.read.format("avro").load(s"$path/VP/Avro/type.avro").toDF()
    RDFDFType.createOrReplaceTempView("type")

    val RDFDFPredicatescombined =spark.read.format("avro").load(s"$path/ST/Avro/SingleStmtTable.avro").toDF()
    RDFDFPredicatescombined.createOrReplaceTempView("SingleStmtTable")



/*
    val RDFDFTitle = spark.read.format("avro").load(s"$path/VP/Avro/title.avro").toDF()
    RDFDFTitle.createOrReplaceTempView("title")

    val RDFDFIssued = spark.read.format("avro").load(s"$path/VP/Avro/issued.avro").toDF()
    RDFDFIssued.createOrReplaceTempView("issued")


    val RDFDFCreator = spark.read.format("avro").load(s"$path/VP/Avro/creator.avro").toDF()
    RDFDFCreator.createOrReplaceTempView("creator")

    val RDFDFBookTitle = spark.read.format("avro").load(s"$path/VP/Avro/booktitle.avro").toDF()
    RDFDFBookTitle.createOrReplaceTempView("booktitle")

    val RDFDFPartOf = spark.read.format("avro").load(s"$path/VP/Avro/partOf.avro").toDF()
    RDFDFPartOf.createOrReplaceTempView("partOf")

    val RDFDFSeeAlso = spark.read.format("avro").load(s"$path/VP/Avro/seeAlso.avro").toDF()
    RDFDFSeeAlso.createOrReplaceTempView("seeAlso")

    val RDFDFPages= spark.read.format("avro").load(s"$path/VP/Avro/pages.avro").toDF()
    RDFDFPages.createOrReplaceTempView("pages")

    val RDFDFHomePage= spark.read.format("avro").load(s"$path/VP/Avro/homepage.avro").toDF()
    RDFDFHomePage.createOrReplaceTempView("homePage")

    val RDFDFAbstract= spark.read.format("avro").load(s"$path/VP/Avro/abstract.avro").toDF()
    RDFDFAbstract.createOrReplaceTempView("abstractv")

    val RDFDFName= spark.read.format("avro").load(s"$path/VP/Avro/name.avro").toDF()
    RDFDFName.createOrReplaceTempView("name")

    val RDFDFJournal= spark.read.format("avro").load(s"$path/VP/Avro/journal.avro").toDF()
    RDFDFJournal.createOrReplaceTempView("journal")

    val RDFDFSubClassOf= spark.read.format("avro").load(s"$path/VP/Avro/subClassOf.avro").toDF()
    RDFDFSubClassOf.createOrReplaceTempView("subClassOf")

    val RDFDFReferencesV= spark.read.format("avro").load(s"$path/VP/Avro/references.avro").toDF()
    RDFDFReferencesV.createOrReplaceTempView("referencesv")

    val RDFDFReferences= spark.read.format("avro").load(s"$path/PT/Avro/Reference.avro").toDF()
    RDFDFReferences.createOrReplaceTempView("reference")

    val RDFDFEditor= spark.read.format("avro").load(s"$path/VP/Avro/editor.avro").toDF()
    RDFDFEditor.createOrReplaceTempView("editorv")
*/

    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/avro/VP/$ds.txt"),true)

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
    println("All Queries are Done - AVRO - VP!")
    
  }
}