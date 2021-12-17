package ee.ut.cs.bigdata.watdiv.querying.parquet


import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.watdiv.querying.queries.ExtVPQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ExtVPTables {
  def main(args: Array[String]): Unit = {


     val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Parquet PT")
      .getOrCreate()
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/"

     //read tables from HDFS

////C1
//val SS_caption_hasReview = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/caption/hasReview.parquet")
//val SS_contentRating_caption=spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/contentRating/caption.parquet")
//val SS_text_caption=spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/text/caption.parquet")
//val SS_hasReview_caption=spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/hasReview/caption.parquet")
////val SO_title_hasReview=spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/") *********NOT FOUND in 10M **********
////INSTEAD WE USE The following (VP	<rev__title>)
//val VP_Rev_title=spark.read.format("parquet").load(s"$path/VP/VHDFS/Parquet/SS/rev_title.parquet")
////val SS_reviewer_title=spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/") *********NOT FOUND in 10M **********
////INSTEAD WE USE The following <rev__reviewer>)
//val VP_Reviewer=spark.read.format("parquet").load(s"$path/VP/VHDFS/Parquet/SS/reviewer.parquet")
//val SS_actor_language=spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/actor/language.parquet")
//val SS_language_actor=spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/language/actor.parquet")

//C1 FOR 10M STRUCTURE
val SS_caption_hasReview = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/caption/hasReview.parquet")
val SS_contentRating_caption=spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/contentRating/caption.parquet")
val SS_text_caption=spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/text/caption.parquet")
val SS_hasReview_caption=spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/hasReview/caption.parquet")
//val SO_title_hasReview=spark.read.format("parquet").load(s"$path/ExtVP/Parquet/") *********NOT FOUND in 10M **********
//INSTEAD WE USE The following (VP	<rev__title>)
val VP_Rev_title=spark.read.format("parquet").load(s"$path/VP/Parquet/rev_title.parquet")
//val SS_reviewer_title=spark.read.format("parquet").load(s"$path/ExtVP/Parquet/") *********NOT FOUND in 10M **********
//INSTEAD WE USE The following <rev__reviewer>)
val VP_Reviewer=spark.read.format("parquet").load(s"$path/VP/Parquet/reviewer.parquet")
val SS_actor_language=spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/actor/language.parquet")
val SS_language_actor=spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/language/actor.parquet")


  //C1
  SS_caption_hasReview.createOrReplaceTempView("SS_caption_hasReview")
  SS_contentRating_caption.createOrReplaceTempView("SS_contentRating_caption")
  SS_text_caption.createOrReplaceTempView("SS_text_caption")
  SS_hasReview_caption.createOrReplaceTempView("SS_hasReview_caption")
  VP_Rev_title.createOrReplaceTempView("VP_Rev_title")
  VP_Reviewer.createOrReplaceTempView("VP_Reviewer")
  SS_actor_language.createOrReplaceTempView("SS_actor_language")
  SS_language_actor.createOrReplaceTempView("SS_language_actor")



   //create file to write the query run time results
//    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/orc/VP/$ds.txt"),true)

    val queries = List(new ExtVPQueries c1_copy)
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
    for (query <- queries) {
      //run query and calculate the run time
      val startTime = System.nanoTime()
      val df_count = spark.sql(query).count()
      println(df_count)
      //df.take(100).foreach(println)
      val endTime = System.nanoTime()
      val result = (endTime - startTime).toDouble / 1000000000

//      //write the result into the log file
//      if (count != queries.size) {
//        Console.withOut(fos) {
//          print(result + ",")
//        }
//      } else {
//        Console.withOut(fos) {
//          println(result)
//        }
//      }
      count += 1
    }
    println("All Queries are Done - Parquet - VP!")



  }
}
