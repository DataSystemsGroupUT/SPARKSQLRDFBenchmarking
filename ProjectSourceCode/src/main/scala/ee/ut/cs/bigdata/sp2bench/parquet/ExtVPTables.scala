package ee.ut.cs.bigdata.sp2bench.parquet

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.ExtVPQueries
import ee.ut.cs.bigdata.sp2bench.queries.VTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ExtVPTables {
  def main(args: Array[String]): Unit = {

    println("ExtVP Partitioned ORC")
    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Parquet ExtVP")
      .getOrCreate()

    println("Spark session is created!")

    import spark.implicits._
    val ds=args(0)			// value = {"100M", "500M, or "1B"} 
    val partitionType = args(1)		// value = {"Horizontal", "Subject", or "Predicate"}
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds"

    println("Reading Tables...")
    //read tables from HDFS
    if(partitionType.toLowerCase == "predicate")
    {

    val vpTable6 = spark.read.format("parquet").load(s"$path/VP/Parquet/type.parquet").toDF()
    val vpTable12 = spark.read.format("parquet").load(s"$path/VP/Parquet/creator.parquet").toDF()
    val vpTable27 = spark.read.format("parquet").load(s"$path/ST/Parquet/SingleStmtTable.parquet").toDF()
    val vpTable29 = spark.read.format("parquet").load(s"$path/VP/Parquet/editor.parquet").toDF()

    val vpTable1 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/issued.parquet").toDF()
    val vpTable2 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/title/issued.parquet").toDF()
    val vpTable3 = spark.read.format("parquet").load(s"$path/VP/Parquet/issued.parquet").toDF()
    val vpTable4 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/pages.parquet").toDF()
    val vpTable5 = spark.read.format("parquet").load(s"$path/VP/Parquet/pages.parquet").toDF()

    val vpTable7 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SO/name/creator.parquet").toDF()
    val vpTable8 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/creator/journal.parquet").toDF()
    val vpTable9 = spark.read.format("parquet").load(s"$path/VP/Parquet/journal.parquet").toDF()
    val vpTable10 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/journal.parquet").toDF()
    val vpTable11 = spark.read.format("parquet").load(s"$path/VP/Parquet/name.parquet").toDF()

    val vpTable13 = spark.read.format("parquet").load(s"$path/VP/Parquet/abstract.parquet").toDF()
    val vpTable14 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/creator/partOf.parquet").toDF()
    val vpTable15 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/booktitle/seeAlso.parquet").toDF()
    val vpTable16 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/partOf/seeAlso.parquet").toDF()
    val vpTable17 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/homepage/partOf.parquet").toDF()                    
    val vpTable18 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/issued/seeAlso.parquet").toDF()
    val vpTable19 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/partOf.parquet").toDF()
    val vpTable20 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/seeAlso/partOf.parquet").toDF()
    val vpTable21 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/pages/partOf.parquet").toDF()
    val vpTable22 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/title/partOf.parquet").toDF()
    val vpTable23 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/creator.parquet").toDF()
    val vpTable24 = spark.read.format("parquet").load(s"$path/VP/Parquet/subClassOf.parquet").toDF()
    val vpTable25 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/issued/creator.parquet").toDF()
    val vpTable26 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/creator/issued.parquet").toDF()
    val vpTable28 = spark.read.format("parquet").load(s"$path/VP/Parquet/seeAlso.parquet").toDF()
    val vpTable30 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/name.parquet").toDF()


      vpTable6.createOrReplaceTempView("VP_Type")
      vpTable27.createOrReplaceTempView("Triples")


      vpTable6.createOrReplaceTempView("VP_Type")
      vpTable12.createOrReplaceTempView("VP_Creator")
      vpTable27.createOrReplaceTempView("Triples")
      vpTable29.createOrReplaceTempView("VP_Editor")
      vpTable9.createOrReplaceTempView("VP_Journal")
      vpTable11.createOrReplaceTempView("VP_Name")

      vpTable1.createOrReplaceTempView("ExtVP_SS_Type_Issued")
      vpTable2.createOrReplaceTempView("ExtVP_SS_Title_Issued")
      vpTable3.createOrReplaceTempView("VP_Issued")
      vpTable4.createOrReplaceTempView("ExtVP_SS_Type_Pages")
      vpTable5.createOrReplaceTempView("VP_Pages")
      vpTable7.createOrReplaceTempView("ExtVP_SO_Name_Creator")
      vpTable8.createOrReplaceTempView("ExtVP_SS_Creator_Journal")
      vpTable9.createOrReplaceTempView("VP_Journal")
      vpTable10.createOrReplaceTempView("ExtVP_SS_Type_Journal")
      vpTable11.createOrReplaceTempView("VP_Name")
      vpTable13.createOrReplaceTempView("VP_Abstract")
      vpTable14.createOrReplaceTempView("ExtVP_SS_Creator_PartOf")
      vpTable15.createOrReplaceTempView("ExtVP_SS_BookTitle_SeeAlso")
      vpTable16.createOrReplaceTempView("ExtVP_SS_PartOf_SeeAlso")
      vpTable17.createOrReplaceTempView("ExtVP_SS_HomePage_PartOf")
      vpTable18.createOrReplaceTempView("ExtVP_SS_Issued_SeeAlso")
      vpTable19.createOrReplaceTempView("ExtVP_SS_Type_PartOf")
      vpTable20.createOrReplaceTempView("ExtVP_SS_SeeAlso_PartOf")
      vpTable21.createOrReplaceTempView("ExtVP_SS_Pages_PartOf")
      vpTable22.createOrReplaceTempView("ExtVP_SS_Title_PartOf")
      vpTable23.createOrReplaceTempView("ExtVP_SS_Type_Creator")
      vpTable24.createOrReplaceTempView("VP_SubClassOf")
      vpTable25.createOrReplaceTempView("ExtVP_SS_Issued_Creator")
      vpTable26.createOrReplaceTempView("ExtVP_SS_Creator_Issued")
      vpTable28.createOrReplaceTempView("VP_SeeAlso")
      vpTable30.createOrReplaceTempView("ExtVP_SS_Type_Name")

    }

    else 
    {
    val vpTable6 = spark.read.format("parquet").load(s"$path/VP/Parquet/type$partitionType.parquet").toDF()
    val vpTable12 = spark.read.format("parquet").load(s"$path/VP/Parquet/creator$partitionType.parquet").toDF()
    val vpTable27 = spark.read.format("parquet").load(s"$path/ST/Parquet/SingleStmtTable$partitionType.parquet").toDF()
    val vpTable29 = spark.read.format("parquet").load(s"$path/VP/Parquet/editor$partitionType.parquet").toDF()

    val vpTable1 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/issued$partitionType.parquet").toDF()
    val vpTable2 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/title/issued$partitionType.parquet").toDF()
    val vpTable3 = spark.read.format("parquet").load(s"$path/VP/Parquet/issued$partitionType.parquet").toDF()
    val vpTable4 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/pages$partitionType.parquet").toDF()
    val vpTable5 = spark.read.format("parquet").load(s"$path/VP/Parquet/pages$partitionType.parquet").toDF()
    val vpTable7 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SO/name/creator$partitionType.parquet").toDF()
    val vpTable8 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/creator/journal$partitionType.parquet").toDF()
    val vpTable9 = spark.read.format("parquet").load(s"$path/VP/Parquet/journal$partitionType.parquet").toDF()
    val vpTable10 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/journal$partitionType.parquet").toDF()
    val vpTable11 = spark.read.format("parquet").load(s"$path/VP/Parquet/name$partitionType.parquet").toDF()
    val vpTable13 = spark.read.format("parquet").load(s"$path/VP/Parquet/abstract$partitionType.parquet").toDF()
    val vpTable14 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/creator/partOf$partitionType.parquet").toDF()
    val vpTable15 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/booktitle/seeAlso$partitionType.parquet").toDF()
    val vpTable16 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/partOf/seeAlso$partitionType.parquet").toDF()
    val vpTable17 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/homepage/partOf$partitionType.parquet").toDF()                    
    val vpTable18 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/issued/seeAlso$partitionType.parquet").toDF()
    val vpTable19 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/partOf$partitionType.parquet").toDF()
    val vpTable20 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/seeAlso/partOf$partitionType.parquet").toDF()
    val vpTable21 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/pages/partOf$partitionType.parquet").toDF()
    val vpTable22 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/title/partOf$partitionType.parquet").toDF()
    val vpTable23 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/creator$partitionType.parquet").toDF()
    val vpTable24 = spark.read.format("parquet").load(s"$path/VP/Parquet/subClassOf$partitionType.parquet").toDF()
    val vpTable25 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/issued/creator$partitionType.parquet").toDF()
    val vpTable26 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/creator/issued$partitionType.parquet").toDF()
    val vpTable28 = spark.read.format("parquet").load(s"$path/VP/Parquet/seeAlso$partitionType.parquet").toDF()
    val vpTable30 = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/name$partitionType.parquet").toDF()



      vpTable6.createOrReplaceTempView("VP_Type")
      vpTable27.createOrReplaceTempView("Triples")


      vpTable6.createOrReplaceTempView("VP_Type")
      vpTable12.createOrReplaceTempView("VP_Creator")
      vpTable27.createOrReplaceTempView("Triples")
      vpTable29.createOrReplaceTempView("VP_Editor")
      vpTable9.createOrReplaceTempView("VP_Journal")
      vpTable11.createOrReplaceTempView("VP_Name")

      vpTable1.createOrReplaceTempView("ExtVP_SS_Type_Issued")
      vpTable2.createOrReplaceTempView("ExtVP_SS_Title_Issued")
      vpTable3.createOrReplaceTempView("VP_Issued")
      vpTable4.createOrReplaceTempView("ExtVP_SS_Type_Pages")
      vpTable5.createOrReplaceTempView("VP_Pages")
      vpTable7.createOrReplaceTempView("ExtVP_SO_Name_Creator")
      vpTable8.createOrReplaceTempView("ExtVP_SS_Creator_Journal")
      vpTable9.createOrReplaceTempView("VP_Journal")
      vpTable10.createOrReplaceTempView("ExtVP_SS_Type_Journal")
      vpTable11.createOrReplaceTempView("VP_Name")
      vpTable13.createOrReplaceTempView("VP_Abstract")
      vpTable14.createOrReplaceTempView("ExtVP_SS_Creator_PartOf")
      vpTable15.createOrReplaceTempView("ExtVP_SS_BookTitle_SeeAlso")
      vpTable16.createOrReplaceTempView("ExtVP_SS_PartOf_SeeAlso")
      vpTable17.createOrReplaceTempView("ExtVP_SS_HomePage_PartOf")
      vpTable18.createOrReplaceTempView("ExtVP_SS_Issued_SeeAlso")
      vpTable19.createOrReplaceTempView("ExtVP_SS_Type_PartOf")
      vpTable20.createOrReplaceTempView("ExtVP_SS_SeeAlso_PartOf")
      vpTable21.createOrReplaceTempView("ExtVP_SS_Pages_PartOf")
      vpTable22.createOrReplaceTempView("ExtVP_SS_Title_PartOf")
      vpTable23.createOrReplaceTempView("ExtVP_SS_Type_Creator")
      vpTable24.createOrReplaceTempView("VP_SubClassOf")
      vpTable25.createOrReplaceTempView("ExtVP_SS_Issued_Creator")
      vpTable26.createOrReplaceTempView("ExtVP_SS_Creator_Issued")
      vpTable28.createOrReplaceTempView("VP_SeeAlso")
      vpTable30.createOrReplaceTempView("ExtVP_SS_Type_Name")

    }


    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/parquet/ExtVP/$ds$partitionType.txt"),true)
 //   val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/parquet/VT/$ds$partitionType.txt"),true)
    val queries = List(
           new ExtVPQueries q1,
		       new ExtVPQueries q2,
		       new ExtVPQueries q3,
		       new ExtVPQueries q4,
		       new ExtVPQueries q5,
		       new ExtVPQueries q6,
		       new ExtVPQueries q8,
		       new ExtVPQueries q9,
		       new ExtVPQueries q10,
		       new ExtVPQueries q11)


    println("Running queries")

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
    println("All Queries are Done - Parquet - ExtVP!") 

  }
}
