package ee.ut.cs.bigdata.sp2bench.hive

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.VTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}

object VerticalTablesPartition {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val warehouseLocation = "hdfs://172.17.77.48:9000/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      //.master("spark://172.17.77.48:7077")
      .appName("rdfbench Hive VT")
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

    val RDFDFTitle = spark.sql("SELECT * FROM title").toDF()
    val RDFDFIssued = spark.sql("SELECT * FROM issued").toDF()
    val RDFDFType = spark.sql("SELECT * FROM type").toDF()
    val RDFDFCreator = spark.sql("SELECT * FROM creator").toDF()
    val RDFDFBookTitle = spark.sql("SELECT * FROM booktitle").toDF()
    val RDFDFPartOf = spark.sql("SELECT * FROM partof").toDF()
    val RDFDFSeeAlso = spark.sql("SELECT * FROM seealso").toDF()
    val RDFDFPages= spark.sql("SELECT * FROM pages").toDF()
    val RDFDFHomePage= spark.sql("SELECT * FROM homepage").toDF()
    val RDFDFAbstractV= spark.sql("SELECT * FROM abstractv").toDF()
    val RDFDFName= spark.sql("SELECT * FROM name").toDF()
    val RDFDFJournal= spark.sql("SELECT * FROM journal").toDF()
    val RDFDFSubClassOf= spark.sql("SELECT * FROM subclassof").toDF()
    val RDFDFReferencesV= spark.sql("SELECT * FROM referencesv").toDF()
    val RDFDFReferences= spark.sql("SELECT * FROM reference").toDF()
    val RDFDFEditorV= spark.sql("SELECT * FROM editorv").toDF()
    val RDFDFPredicatescombined= spark.sql("SELECT * FROM singlestmttable").toDF()
    println("Original tables loaded!")

    //partition and save into Hive
    if(partitionType == "subject")
    {
      hiveDB = hiveDB+partitionType 
      spark.sql(s"USE $hiveDB")
      RDFDFTitle.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".title")
      RDFDFIssued.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".issued")
      RDFDFType.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".type")
      RDFDFCreator.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".creator")
      RDFDFBookTitle.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".booktitle")
      RDFDFPartOf.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".partof")
      RDFDFSeeAlso.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".seealso")
      RDFDFPages.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".pages")
      RDFDFHomePage.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".homepage")
      RDFDFAbstractV.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".abstractv")
      RDFDFName.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".name")
      RDFDFJournal.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".journal")
      RDFDFSubClassOf.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".subclassof")
      RDFDFReferencesV.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".referencesv")
      RDFDFReferences.repartition(84, $"document").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".reference")
      RDFDFEditorV.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".editorv")
      RDFDFPredicatescombined.repartition(84, $"Subject").write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".singlestmttable")
      println("Hive VT partitioned and saved! Subject based partitioning!") 
    }

    else if (partitionType == "horizontal")
    {
      hiveDB = hiveDB+partitionType 
      spark.sql(s"USE $hiveDB")

      RDFDFTitle.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".title")
      RDFDFIssued.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".issued")
      RDFDFType.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".type")
      RDFDFCreator.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".creator")
      RDFDFBookTitle.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".booktitle")
      RDFDFPartOf.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".partof")
      RDFDFSeeAlso.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".seealso")
      RDFDFPages.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".pages")
      RDFDFHomePage.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".homepage")
      RDFDFAbstractV.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".abstractv")
      RDFDFName.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".name")
      RDFDFJournal.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".journal")
      RDFDFSubClassOf.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".subclassof")
      RDFDFReferencesV.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".referencesv")
      RDFDFReferences.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".reference")
      RDFDFEditorV.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".editorv")
      RDFDFPredicatescombined.repartition(84).write.option("header", "true").mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB"+".singlestmttable")
      println("Hive VT partitioned and saved! Horizontal partitioning!") 
    }         

  }
}
