package ee.ut.cs.bigdata.watdiv.partitioning.avro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SingleStatementTablePartition {
  def main(args: Array[String]): Unit = {

     val conf = new SparkConf()
     Logger.getLogger("org").setLevel(Level.OFF)
     Logger.getLogger("akka").setLevel(Level.OFF)
     val sc = new SparkContext(conf)

     sc.setLogLevel("ERROR")

     val spark = SparkSession
      .builder()
      .appName("RDFBench Avro ST")
      .getOrCreate()
    
     import spark.implicits._
	      
     val ds=args(0)						// data size
     var partitionType=args(1).toLowerCase 			// horizontal, predicate or subject
     val path= s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/ST"

     //read original table
     val RDFDF = spark.read.format("avro").load(s"$path/VHDFS/Avro/ST$ds.avro").toDF()

     //partition and save on HDFS
     if(partitionType == "subject")
     {
       RDFDF.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/Subject/Avro/ST$ds.avro")
       println("Avro ST partitioned and saved! Subject!")
     }

     else if (partitionType == "predicate")
     {
       RDFDF.repartition(84, $"Predicate").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/Predicate/Avro/ST$ds.avro")
       println("Avro ST partitioned and saved! Predicate!")
     }

     else if (partitionType == "horizontal")
     {
       RDFDF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/Horizontal/Avro/ST$ds.avro")
       println("Avro ST partitioned and saved! Horizontal!")
     }

  }
}
