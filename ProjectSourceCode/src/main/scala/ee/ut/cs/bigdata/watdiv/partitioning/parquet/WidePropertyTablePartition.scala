package ee.ut.cs.bigdata.watdiv.partitioning.parquet

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object WidePropertyTablePartition {
  def main(args: Array[String]): Unit = {

     val conf = new SparkConf()
     Logger.getLogger("org").setLevel(Level.OFF)
     Logger.getLogger("akka").setLevel(Level.OFF)
     val sc = new SparkContext(conf)

     sc.setLogLevel("ERROR")

     val spark = SparkSession
      .builder()
      .appName("RDFBench Parquet WPT Partitioning")
      .getOrCreate()
    
     import spark.implicits._
	      
     val ds=args(0)						// data size
     var partitionType=args(1).toLowerCase 			// horizontal, predicate or subject
     val path= s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/WPT"

     //read original table
     val WPTDF = spark.read.format("parquet").load(s"$path/VHDFS/Parquet/WidePropertyTable.parquet").toDF()

     //partition and save on HDFS
     if(partitionType == "subject")
     {
       WPTDF.repartition(84, $"s").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/Subject/Parquet/WPT$ds.parquet")
       println("Parquet WPT partitioned and saved Subject!")
     }

     else if (partitionType == "horizontal")
     {
       WPTDF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(s"$path/Horizontal/Parquet/WPT$ds.parquet")
       println("Parquet WPT partitioned and saved Horizontal!")
     }

  }
}
