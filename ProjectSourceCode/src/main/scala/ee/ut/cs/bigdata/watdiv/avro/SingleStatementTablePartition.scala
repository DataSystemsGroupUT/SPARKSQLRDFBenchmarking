package ee.ut.cs.bigdata.watdiv.avro

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
     val path= s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/ST/Avro"

     //read original table
     val RDFDF = spark.read.format("avro").load(s"$path/SingleStmtTable").toDF()

     //partition and save on HDFS
     if(partitionType == "subject")
     {
       RDFDF.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/SingleStmtTableSubject.avro")
       println("Avro ST partitioned and saved! Subject!")
     }

     else if (partitionType == "predicate")
     {
       RDFDF.repartition(84, $"Predicate").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/SingleStmtTablePredicate.avro")
       println("Avro ST partitioned and saved! Predicate!")
     }

     else if (partitionType == "horizontal")
     {
       RDFDF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(s"$path/SingleStmtTableHorizontal.avro")
       println("Avro ST partitioned and saved! Horizontal!")
     }

  }
}
