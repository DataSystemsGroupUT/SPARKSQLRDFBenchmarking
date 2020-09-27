package ee.ut.cs.bigdata.sp2bench.hive

import java.io.{File, FileOutputStream}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ExtVPTablesPartition {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val warehouseLocation = "hdfs://172.17.77.48:9000/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName("rdfbench Hive ExtVP")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris","thrift://172.17.77.48:9083")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._ 
     
    val db = "rdfbench"
    val ds = args(0)					// data size
    var partitionType=args(1).toLowerCase	// horizontal, predicate or subject

    //use Hive database to read non-partitioned tables
    var hiveDB= db.concat(ds)    
    spark.sql(s"USE $hiveDB")
    
    println("Reading HIVE tables")
    //read original tables

    val vpTable9 = spark.sql("SELECT * FROM vp_journal").toDF()
    val vpTable11 = spark.sql("SELECT * FROM vp_name").toDF()
    val vpTable6 = spark.sql("SELECT * FROM vp_type").toDF()
    val vpTable12 = spark.sql("SELECT * FROM vp_creator").toDF()
    val vpTable27 = spark.sql("SELECT * FROM triples").toDF()
    val vpTable29 = spark.sql("SELECT * FROM vp_editor").toDF()

/*
    val vpTable28 = spark.sql("SELECT * FROM vp_seealso").toDF()

    val vpTable1 = spark.sql("SELECT * FROM extvp_ss_type_issued").toDF()
    val vpTable2 = spark.sql("SELECT * FROM extvp_ss_title_issued").toDF()
    val vpTable3 = spark.sql("SELECT * FROM vp_issued").toDF()
    val vpTable4 = spark.sql("SELECT * FROM extvp_ss_type_pages").toDF()
    val vpTable5 = spark.sql("SELECT * FROM vp_pages").toDF()
    val vpTable7 = spark.sql("SELECT * FROM extvp_so_name_creator").toDF()
    val vpTable8 = spark.sql("SELECT * FROM extvp_ss_creator_journal").toDF()
    val vpTable10 = spark.sql("SELECT * FROM extvp_ss_type_journal").toDF()
    val vpTable13 = spark.sql("SELECT * FROM vp_abstract").toDF()
    val vpTable14 = spark.sql("SELECT * FROM extvp_ss_creator_partof").toDF()
    val vpTable15 = spark.sql("SELECT * FROM extvp_ss_booktitle_seealso").toDF()
    val vpTable16 = spark.sql("SELECT * FROM extvp_ss_partof_seealso").toDF()
    val vpTable17 = spark.sql("SELECT * FROM extvp_ss_homepage_partof").toDF()
    val vpTable18 = spark.sql("SELECT * FROM extvp_ss_issued_seealso").toDF()
    val vpTable19 = spark.sql("SELECT * FROM extvp_ss_type_partof").toDF()
    val vpTable20 = spark.sql("SELECT * FROM extvp_ss_seealso_partof").toDF()
    val vpTable21 = spark.sql("SELECT * FROM extvp_ss_pages_partof").toDF()
    val vpTable22 = spark.sql("SELECT * FROM extvp_ss_title_partof").toDF()
    val vpTable23 = spark.sql("SELECT * FROM extvp_ss_type_creator").toDF()
    val vpTable24 = spark.sql("SELECT * FROM vp_subclassof").toDF()
    val vpTable25 = spark.sql("SELECT * FROM extvp_ss_issued_creator").toDF()
    val vpTable26 = spark.sql("SELECT * FROM extvp_ss_creator_issued").toDF()
    val vpTable30 = spark.sql("SELECT * FROM extvp_ss_type_name").toDF()
*/
    println("Reading finished!")
    

    //partition and save into HIVE
    if(partitionType == "subject")
    {
      hiveDB = hiveDB+partitionType 
      spark.sql(s"USE $hiveDB")

     print("Partitioning Subject based: ")

     vpTable6.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".type")
     vpTable12.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".creator")
     vpTable27.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".singlestmttable")
     vpTable29.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".editorv")
     vpTable9.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".journal")
     vpTable11.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".name")



/*
     vpTable6.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_type")
     vpTable12.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_creator")
     vpTable27.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".triples")
     vpTable28.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_seealso")
     vpTable29.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_editor")

     vpTable1.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_type_issued")
     vpTable2.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_title_issued")
     vpTable3.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_issued")
     vpTable4.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_type_pages")
     vpTable5.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_pages")
     vpTable7.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_so_name_creator")
     vpTable8.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_creator_journal")
     vpTable9.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_journal")
     vpTable10.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_type_journal")
     vpTable11.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_name")
     vpTable13.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_abstract")
     vpTable14.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_creator_partof")
     vpTable15.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_booktitle_seealso")
     vpTable16.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_partof_seealso")
     vpTable17.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_homepage_partof")
     vpTable18.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_issued_seealso")
     vpTable19.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_type_partof")
     vpTable20.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_seealso_partof")
     vpTable21.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_pages_partof")
     vpTable22.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_title_partof")
     vpTable23.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_type_creator")
     vpTable24.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_subclassof")
     vpTable25.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_issued_creator")
     vpTable26.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_creator_issued")
     vpTable30.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_type_name")
*/
     println("HIVE ExtVP partitioned and saved! Subject!") 
    }

    else if (partitionType == "horizontal")
    {
      hiveDB = hiveDB+partitionType 
      spark.sql(s"USE $hiveDB")

     print("Partitioning Horizontally: ")

     vpTable6.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".type")
     vpTable12.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".creator")
     vpTable27.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".singlestmttable")
     vpTable29.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".editorv")
     vpTable9.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".journal")
     vpTable11.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".name")


/*
     vpTable6.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_type")
     vpTable12.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_creator")
     vpTable27.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".triples")
     vpTable28.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_seealso")
     vpTable29.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_editor")

     vpTable1.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_type_issued")
     vpTable2.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_title_issued")
     vpTable3.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_issued")
     vpTable4.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_type_pages")
     vpTable5.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_pages")
     vpTable7.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_so_name_creator")
     vpTable8.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_creator_journal")
     vpTable9.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_journal")
     vpTable10.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_type_journal")
     vpTable11.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_name")

     vpTable13.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_abstract")
     vpTable14.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_creator_partof")
     vpTable15.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_booktitle_seealso")
     vpTable16.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_partof_seealso")
     vpTable17.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_homepage_partof")
     vpTable18.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_issued_seealso")
     vpTable19.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_type_partof")
     vpTable20.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_seealso_partof")
     vpTable21.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_pages_partof")
     vpTable22.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_title_partof")
     vpTable23.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_type_creator")
     vpTable24.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".vp_subclassof")
     vpTable25.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_issued_creator")
     vpTable26.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_creator_issued")
     vpTable30.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".extvp_ss_type_name")

*/
     println("HIVE ExtVP partitioned and saved! Horizontal!") 
   }

}
}
