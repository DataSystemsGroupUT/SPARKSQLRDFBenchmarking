package ee.ut.cs.bigdata.sp2bench.hive

import java.io.{File, FileOutputStream}
import java.util.concurrent.TimeoutException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}

object WPTTablesPartition {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val warehouseLocation = "hdfs://172.17.77.48:9000/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName("rdfbench Hive WPT")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris","thrift://172.17.77.48:9083")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._ 
        
    val db = "rdfbench"
    val ds = args(0) 				// data size
    var partitionType=args(1).toLowerCase	// horizontal, predicate or subject

    //use original db
    var hiveDB= db.concat(ds)
    spark.sql(s"USE $hiveDB") 

    //read tables from Hive
    val RDFDFWPT = spark.sql("SELECT * FROM wpt").toDF()

    //partition and save into Hive
    if(partitionType.toLowerCase == "subject")
    {
      hiveDB = hiveDB+partitionType 
      spark.sql(s"USE $hiveDB")

      RDFDFWPT.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wpt")
      println("Hive WPT partitioned and saved! Subject!")
    }

    else if (partitionType.toLowerCase == "horizontal")
    {
      hiveDB = hiveDB+partitionType 
      spark.sql(s"USE $hiveDB")

      RDFDFWPT.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wpt")  
      println("Hive WPT partitioned and saved! Horizontal!")   
     }
    
    else if (partitionType.toLowerCase == "predicate")
    {
     println("reading hive tables")

     // read splitted wpt tables
     val wpt1 = spark.sql("SELECT * FROM wptabstract").toDF()
     val wpt2 = spark.sql("SELECT * FROM wptbooktitle").toDF()
     val wpt3 = spark.sql("SELECT * FROM wptcdrom").toDF()
     val wpt4 = spark.sql("SELECT * FROM wptcreator").toDF()
     val wpt5 = spark.sql("SELECT * FROM wpteditor").toDF()
     val wpt6 = spark.sql("SELECT * FROM wpthomepage").toDF()
     val wpt7 = spark.sql("SELECT * FROM wptisbn").toDF()
     val wpt8 = spark.sql("SELECT * FROM wptissued").toDF()
     val wpt9 = spark.sql("SELECT * FROM wptjournal").toDF()
     val wpt10 = spark.sql("SELECT * FROM wptmonth").toDF()
     val wpt11 = spark.sql("SELECT * FROM wptname").toDF()
     val wpt12 = spark.sql("SELECT * FROM wptnote").toDF()
     val wpt13 = spark.sql("SELECT * FROM wptnumber").toDF()
     val wpt14 = spark.sql("SELECT * FROM wptpages").toDF()
     val wpt15 = spark.sql("SELECT * FROM wptpartof").toDF()
     val wpt16 = spark.sql("SELECT * FROM wptpublisher").toDF()
     val wpt17 = spark.sql("SELECT * FROM wptreferences").toDF()
     val wpt18 = spark.sql("SELECT * FROM wptseealso").toDF()
     val wpt19 = spark.sql("SELECT * FROM wptseries").toDF()
     val wpt20 = spark.sql("SELECT * FROM wptsubclassof").toDF()
     val wpt21 = spark.sql("SELECT * FROM wpttitle").toDF()
     val wpt22 = spark.sql("SELECT * FROM wpttype").toDF()
     val wpt23 = spark.sql("SELECT * FROM wptvolume").toDF()

     //partition and save into Hive
     hiveDB = hiveDB + partitionType
     spark.sql(s"USE $hiveDB")
     println("Partitioning tables")
     wpt1.repartition(84, $"abstract").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptabstract")
     wpt2.repartition(84, $"booktitle").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptbooktitle")
     wpt3.repartition(84, $"cdrom").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptcdrom")
     wpt4.repartition(84, $"creator").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptcreator")
     wpt5.repartition(84, $"editor").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wpteditor")
     wpt6.repartition(84, $"homepage").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wpthomepage")
     wpt7.repartition(84, $"isbn").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptisbn")
     wpt8.repartition(84, $"issued").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptissued")
     wpt9.repartition(84, $"journal").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptjournal")
     wpt10.repartition(84, $"month").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptmonth")
     wpt11.repartition(84, $"name").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptname")
     wpt12.repartition(84, $"note").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptnote")
     wpt13.repartition(84, $"number").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptnumber")
     wpt14.repartition(84, $"pages").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptpages")
     wpt15.repartition(84, $"partof").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptpartof")
     wpt16.repartition(84, $"publisher").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptpublisher")
     wpt17.repartition(84, $"references").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptreferences")
     wpt18.repartition(84, $"seealso").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptseealso")
     wpt19.repartition(84, $"series").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptseries")
     wpt20.repartition(84, $"subclassof").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptsubclassof")
     wpt21.repartition(84, $"title").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wpttitle")
     wpt22.repartition(84, $"type").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wpttype")
     wpt23.repartition(84, $"volume").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".wptvolume")

     println("Hive WPT partitioned and saved! predicate!")
    }

  }
}

