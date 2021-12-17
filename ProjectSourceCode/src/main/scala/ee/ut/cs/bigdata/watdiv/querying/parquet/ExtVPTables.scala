package ee.ut.cs.bigdata.watdiv.querying.parquet


import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.ExtVPQueries
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
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/ExtVP/"

     //read tables from HDFS

val SS_caption_hasReview = spark.read.format("parquet").load(s"$path/VHDFS/Parquet")
val SS_contentRating_caption=spark.read.format("parquet").load(s"$path/VHDFS/Parquet")
val SS_text_caption=spark.read.format("parquet").load(s"$path/VHDFS/Parquet")
val SS_hasReview_caption=spark.read.format("parquet").load(s"$path/VHDFS/Parquet")
val SO_title_hasReview=spark.read.format("parquet").load(s"$path/VHDFS/Parquet")
val SS_reviewer_title=spark.read.format("parquet").load(s"$path/VHDFS/Parquet")
val SS_actor_language=spark.read.format("parquet").load(s"$path/VHDFS/Parquet")
val SS_language_actor=spark.read.format("parquet").load(s"$path/VHDFS/Parquet")















  }

}
