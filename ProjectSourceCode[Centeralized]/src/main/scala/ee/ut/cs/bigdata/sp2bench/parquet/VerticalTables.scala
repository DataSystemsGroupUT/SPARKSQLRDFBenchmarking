package ee.ut.cs.bigdata.sp2bench.parquet


  import java.io.{File, FileOutputStream}

  import ee.ut.cs.bigdata.sp2bench.queries.VTQueries
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.{SparkConf, SparkContext}

object VerticalTables {

  def main(args: Array[String]): Unit = {




    val conf = new SparkConf().setMaster("local").setAppName("SQLSPARK")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")



    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SparkSessionZipsExample")
      .getOrCreate()


    val ds="100K"
    val path=s"hdfs://quickstart:8020/user/cloudera/RDFBench/SP2B/$ds/Parquet"


    val RDFDFTitle = spark.read.format("parquet").load(s"$path/VerticalTables/title.parquet").toDF()
    RDFDFTitle.createOrReplaceTempView("title")


    val RDFDFIssued = spark.read.format("parquet").load(s"$path/VerticalTables/issued.parquet").toDF()
    RDFDFIssued.createOrReplaceTempView("issued")


    val RDFDFType = spark.read.format("parquet").load(s"$path/VerticalTables/type.parquet").toDF()
    RDFDFType.createOrReplaceTempView("type")


    val RDFDFCreator = spark.read.format("parquet").load(s"$path/VerticalTables/creator.parquet").toDF()
    RDFDFCreator.createOrReplaceTempView("creator")


    val RDFDFBookTitle = spark.read.format("parquet").load(s"$path/VerticalTables/booktitle.parquet").toDF()
    RDFDFBookTitle.createOrReplaceTempView("booktitle")


    val RDFDFPartOf = spark.read.format("parquet").load(s"$path/VerticalTables/partof.parquet").toDF()
    RDFDFPartOf.createOrReplaceTempView("partOf")


    val RDFDFSeeAlso = spark.read.format("parquet").load(s"$path/VerticalTables/seealso.parquet").toDF()
    RDFDFSeeAlso.createOrReplaceTempView("seeAlso")



    val RDFDFPages= spark.read.format("parquet").load(s"$path/VerticalTables/pages.parquet").toDF()
    RDFDFPages.createOrReplaceTempView("pages")


    val RDFDFHomePage= spark.read.format("parquet").load(s"$path/VerticalTables/homepage.parquet").toDF()
    RDFDFHomePage.createOrReplaceTempView("homePage")


    val RDFDFAbstract= spark.read.format("parquet").load(s"$path/VerticalTables/abstract.parquet").toDF()
    RDFDFAbstract.createOrReplaceTempView("abstractv")


    val RDFDFName= spark.read.format("parquet").load(s"$path/VerticalTables/name.parquet").toDF()
    RDFDFName.createOrReplaceTempView("name")


    val RDFDFJournal= spark.read.format("parquet").load(s"$path/VerticalTables/injournal.parquet").toDF()
    RDFDFJournal.createOrReplaceTempView("journal")


    val RDFDFSubClassOf= spark.read.format("parquet").load(s"$path/VerticalTables/subclassof.parquet").toDF()
    RDFDFSubClassOf.createOrReplaceTempView("subClassOf")


    val RDFDFReferencesV= spark.read.format("parquet").load(s"$path/VerticalTables/references.parquet").toDF()
    RDFDFReferencesV.createOrReplaceTempView("referencesv")


    val RDFDFReferences= spark.read.format("parquet").load(s"$path/PropertyTables/Reference.parquet").toDF()
    RDFDFReferences.createOrReplaceTempView("reference")

    val RDFDFEditor= spark.read.format("parquet").load(s"$path/VerticalTables/editor.parquet").toDF()
    RDFDFEditor.createOrReplaceTempView("editorv")

    val RDFDFpredicatescombined= spark.read.format("parquet").load(s"$path/SingleStmtTable").toDF()
    RDFDFpredicatescombined.createOrReplaceTempView("SingleStmtTable")


    val fos = new FileOutputStream(new File(s"/home/cloudera/Downloads/Results/parquet/VT/$ds.txt"),true)


    Console.withOut(fos) {"Q1:"+spark.time(spark.sql(new VTQueries q1).show)}
    Console.withOut(fos) {"Q2:"+spark.time(spark.sql(new VTQueries q2).show)}
    Console.withOut(fos) {"Q3:"+spark.time(spark.sql(new VTQueries q3).show)}
    Console.withOut(fos) {"Q4:"+spark.time(spark.sql(new VTQueries q4).show)}
    Console.withOut(fos) {"Q5:"+spark.time(spark.sql(new VTQueries q5).show)}
    Console.withOut(fos) {"Q6:"+spark.time(spark.sql(new VTQueries q6).show)}
    Console.withOut(fos) {"Q8:"+spark.time(spark.sql(new VTQueries q8).show)}
    Console.withOut(fos) {"Q9:"+spark.time(spark.sql(new VTQueries q9).show)}
    Console.withOut(fos) {"Q10:"+spark.time(spark.sql(new VTQueries q10).show)}
    Console.withOut(fos) {"Q11:"+spark.time(spark.sql(new VTQueries q11).show)}

    Console.withOut(fos) {"Q7:"+spark.time(spark.sql(new VTQueries q7).show())}

    Console.withOut(fos) {println("=======================================")}
    println("All Queries are Done!")




  }

}
