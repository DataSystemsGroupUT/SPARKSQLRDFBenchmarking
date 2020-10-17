package ee.ut.cs.bigdata.sp2bench.csv

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.WPTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel._


object WPTTables {
  def main(args: Array[String]): Unit = {
    println("queries")
    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench CSV WPT")
      .getOrCreate()
    println("Spark Session created!")

    import spark.implicits._
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val partitionType = args(1) //value = {"Horizontal", "Subject", or "Predicate"}
    val hdfsURL = "hdfs://172.17.77.48:9000"

    val path = s"$hdfsURL/user/hadoop/RDFBench/SP2B/$ds/WPT/CSV"

    //read tables from HDFS
    if (partitionType.toLowerCase == "predicate") {
      //read splitted partitioned WPT tables
      import org.apache.spark.sql.functions._
      println("reading tables")
      val wpt1 = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(s"$path/WPTType$partitionType.csv")
        .toDF()
        .groupBy("Subject")
        .agg(collect_list("type") as "type")

      val wpt2 = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(s"$path/WPTName$partitionType.csv")
        .toDF()
        .groupBy("Subject")
        .agg(collect_list("name") as "name")

      val wpt3 = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(s"$path/WPTTitle$partitionType.csv")
        .toDF()
        .groupBy("Subject")
        .agg(collect_list("title") as "title")

      val wpt4 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTIssued$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("issued") as "issued")

      val wpt5 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTCreator$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("creator") as "creator")

      val wpt6 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTHomepage$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("homepage") as "homepage")

      val wpt7 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTSeeAlso$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("seeAlso") as "seeAlso")

      val wpt8 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTBooktitle$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("booktitle") as "booktitle")

      val wpt9 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTPartOf$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("partOf") as "partOf")

      val wpt10 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTAbstract$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("abstract") as "abstract")

      val wpt11 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTPages$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("pages") as "pages")

      val wpt12 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTJournal$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("journal") as "journal")

      val wpt13 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTSeries$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("series") as "series")

      val wpt14 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTNumber$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("number") as "number")

      val wpt15 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTNote$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("note") as "note")

      val wpt16 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTVolume$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("volume") as "volume")

      val wpt17 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTSubClassOf$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("subClassOf") as "subClassOf")

      val wpt18 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTMonth$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("month") as "month")

      val wpt19 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTIsbn$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("isbn") as "isbn")

      val wpt20 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTEditor$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("editor") as "editor")

      val wpt21 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTPublisher$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("publisher") as "publisher")

      val wpt22 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTReferences$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("references") as "references")

      val wpt23 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPTCdrom$partitionType.csv").toDF()
        .groupBy("Subject")
        .agg(collect_list("cdrom") as "cdrom")

      println("tables are read")

      //join WPT tables based on 'Subject' column
      val wpt_join1 = wpt1.join(wpt2, wpt1("Subject") === wpt2("Subject")).drop(wpt2("Subject"))
      val wpt_join2 = wpt_join1.join(wpt3, wpt_join1("Subject") === wpt3("Subject")).drop(wpt3("Subject"))
      val wpt_join3 = wpt_join2.join(wpt4, wpt_join2("Subject") === wpt4("Subject")).drop(wpt4("Subject"))
      val wpt_join4 = wpt_join3.join(wpt5, wpt_join3("Subject") === wpt5("Subject")).drop(wpt5("Subject"))
      val wpt_join5 = wpt_join4.join(wpt6, wpt_join4("Subject") === wpt6("Subject")).drop(wpt6("Subject"))
      val wpt_join6 = wpt_join5.join(wpt7, wpt_join5("Subject") === wpt7("Subject")).drop(wpt7("Subject"))
      val wpt_join7 = wpt_join6.join(wpt8, wpt_join6("Subject") === wpt8("Subject")).drop(wpt8("Subject"))
      val wpt_join8 = wpt_join7.join(wpt9, wpt_join7("Subject") === wpt9("Subject")).drop(wpt9("Subject"))
      val wpt_join9 = wpt_join8.join(wpt10, wpt_join8("Subject") === wpt10("Subject")).drop(wpt10("Subject"))
      val wpt_join10 = wpt_join9.join(wpt11, wpt_join9("Subject") === wpt11("Subject")).drop(wpt11("Subject"))
      val wpt_join11 = wpt_join10.join(wpt12, wpt_join10("Subject") === wpt12("Subject")).drop(wpt12("Subject"))
      val wpt_join12 = wpt_join11.join(wpt13, wpt_join11("Subject") === wpt13("Subject")).drop(wpt13("Subject"))
      val wpt_join13 = wpt_join12.join(wpt14, wpt_join12("Subject") === wpt14("Subject")).drop(wpt14("Subject"))
      val wpt_join14 = wpt_join13.join(wpt15, wpt_join13("Subject") === wpt15("Subject")).drop(wpt15("Subject"))
      val wpt_join15 = wpt_join14.join(wpt16, wpt_join14("Subject") === wpt16("Subject")).drop(wpt16("Subject"))
      val wpt_join16 = wpt_join15.join(wpt17, wpt_join15("Subject") === wpt17("Subject")).drop(wpt17("Subject"))
      val wpt_join17 = wpt_join16.join(wpt18, wpt_join16("Subject") === wpt18("Subject")).drop(wpt18("Subject"))
      val wpt_join18 = wpt_join17.join(wpt19, wpt_join17("Subject") === wpt19("Subject")).drop(wpt19("Subject"))
      val wpt_join19 = wpt_join18.join(wpt20, wpt_join18("Subject") === wpt20("Subject")).drop(wpt20("Subject"))
      val wpt_join20 = wpt_join19.join(wpt21, wpt_join19("Subject") === wpt21("Subject")).drop(wpt21("Subject"))
      val wpt_join21 = wpt_join20.join(wpt22, wpt_join20("Subject") === wpt22("Subject")).drop(wpt22("Subject"))
      val wpt_join22 = wpt_join21.join(wpt23, wpt_join21("Subject") === wpt23("Subject")).drop(wpt23("Subject"))
      println("Tables are joined")

      val result = wpt_join22
        .withColumn(
          "tmp",
          arrays_zip(
            col("type"),
            col("name"),
            col("title"),
            col("issued"),
            col("creator"),
            col("homepage"),
            col("seeAlso"),
            col("booktitle"),
            col("partOf"),
            col("abstract"),
            col("pages"),
            col("journal"),
            col("series"),
            col("number"),
            col("note"),
            col("volume"),
            col("subClassOf"),
            col("month"),
            col("isbn"),
            col("editor"),
            col("publisher"),
            col("references"),
            col("cdrom")))
        .withColumn("tmp", explode(col("tmp")))
        .select(
          col("Subject"),
          col("tmp.type"),
          col("tmp.name"),
          col("tmp.title"),
          col("tmp.issued"),
          col("tmp.creator"),
          col("tmp.homepage"),
          col("tmp.seeAlso"),
          col("tmp.booktitle"),
          col("tmp.partOf"),
          col("tmp.abstract"),
          col("tmp.pages"),
          col("tmp.journal"),
          col("tmp.series"),
          col("tmp.number"),
          col("tmp.note"),
          col("tmp.volume"),
          col("tmp.subClassOf"),
          col("tmp.month"),
          col("tmp.isbn"),
          col("tmp.editor"),
          col("tmp.publisher"),
          col("tmp.references"),
          col("tmp.cdrom"))

      println("Unzipped")

      result.cache()
      result.createOrReplaceTempView("WPT")

      spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WidePropertyTable.csv").toDF().createOrReplaceTempView("WPT2")
      println("Number of rows on 100K WPT default:")
      spark.sql("select count(*) from WPT2").show()

      //result.coalesce(1).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTJoined.csv")
      println("Number of rows on 100K WPT joined:")
      spark.sql("select count(*) from WPT").show()
    } else {
      val RDFDFWPT = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/WPT$partitionType.csv").toDF()
      RDFDFWPT.createOrReplaceTempView("WPT")
    }

    //create file to write the query run time results    
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/csv/WPT/$ds$partitionType.txt"), true)

    val queries = List(
      new WPTQueries q1,
      new WPTQueries q2,
      new WPTQueries q3,
      //      new WPTQueries q4,
      new WPTQueries q5,
      new WPTQueries q6,
      new WPTQueries q8,
      new WPTQueries q10,
      new WPTQueries q11,
    )

    var count = 1
    for (query <- queries) {
      //run query and calculate the run time
      val starttime = System.nanoTime()
      val df = spark.sql(query)
      df.take(100).foreach(println)
      val endtime = System.nanoTime()
      val result = (endtime - starttime).toDouble / 1000000000

      //write the result into the log file
      if (count != queries.size) {
        Console.withOut(fos) {
          print(result + ",")
        }
      } else {
        Console.withOut(fos) {
          println(result)
        }
      }
      count += 1
    }
    println("All Queries are Done - CSV - WPT!")
  }
}

//new test 
/*result.coalesce(1).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(s"$path/WPTJoined.csv")
println("100K number of rows")
spark.sql("select count(*) from WPT").show()*/

/*  println("loading tables")
    import org.apache.spark.sql.functions._
    val df_Creator = spark.sparkContext.parallelize(Seq(
      ("Pe1",""),
      ("Pe2",""),
      ("Article1","Ragab"),
      ("Article1","Riccy"),
      ("Article1","Sadiq"),
      ("Article2","Ragab"),
      ("Article3","Omar"),
      ("Article3","Gaber"),
      ("inproc1","Omar"),
      ("inproc1","Gaber"),
      ("journal1",""),
      ("journal2","") 
    )).toDF("Subject","Author")

    val df_creator2= df_Creator
      .groupBy("Subject")
      .agg(collect_list("Author") as "Author")


    val df_Type = spark.sparkContext.parallelize(Seq(
      ("Pe1","Person"), 
      ("Pe2","Person"),
      ("Article1","Article"), 
      ("Article1","Article"),
      ("Article1","Article"), 
      ("Article2","Article"),
      ("Article3","Article"), 
      ("Article3","Article"),
      ("inproc1","InProceeding"), 
      ("inproc1","InProceeding"),
      ("journal1","Journal"),
      ("journal2","Journal")
    )).toDF("Subject","Type")

    val df_Type2= df_Type
      .groupBy("Subject")
      .agg(collect_list("Type") as "Type")


    val df_editor = spark.sparkContext.parallelize(Seq(
      ("Pe1",""), 
      ("Pe2",""),
      ("Article1",""), 
      ("Article1",""),
      ("Article1",""), 
      ("Article2",""),
      ("Article3",""), 
      ("Article3",""),
      ("inproc1",""), 
      ("inproc1",""),
      ("journal1","Paul_erdoes"),
      ("journal2","Ragab")
    )).toDF("Subject","Editor")

    val df_Editor2= df_editor
      .groupBy("Subject")
      .agg(collect_list("Editor") as "Editor")

    val df2=df_Type2.join(df_creator2, df_Type2.col("Subject").equalTo(df_creator2("Subject"))).drop(df_creator2("Subject"))
    val df3=df2.join(df_Editor2, df2.col("Subject").equalTo(df_Editor2("Subject"))).drop(df_Editor2("Subject"))

    println("Grouped and Joined")
    df3.show(1000)

    println("Unzipped")
    df3
       .withColumn("tmp", arrays_zip(col("Author"), col("Type"), col("Editor")))
       .withColumn("tmp", explode(col("tmp")))
       .select(col("Subject"), col("tmp.Author"), col("tmp.Type"), col("tmp.Editor")).show() */

