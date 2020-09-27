package ee.ut.cs.bigdata.sp2bench

import java.io.{File, FileOutputStream}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel._

object CreateWPTTable {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Create WPT")
      .getOrCreate()

    println("Spark Session is created")

    import spark.implicits._
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
        
    val ds=args(0)		 		// value = {"100K", "100M", "500M, or "1B"} 
    val path=s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/SP2B/$ds"
                     
    //read tables from HDFS
    val RDFDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/CSV/ST/SingleStmtTable.csv").toDF()
    RDFDF.createOrReplaceTempView("triples")
    println("Table is read")       

    var wptTable = spark.sql(
      """
        |select DISTINCT Pred1.Subject, Pred1.type, Pred2.name, pred3.title, Pred4.issued, Pred5.creator, Pred6.homepage, Pred7.seeAlso,
        |Pred8.booktitle, Pred9.partOf, Pred10.abstract, Pred11.pages, Pred12.journal, Pred13.series, Pred14.number, Pred15.note,
        |Pred16.volume,Pred17.subClassOf, Pred18.month, Pred19.isbn,Pred20.editor, Pred21.publisher, Pred22.references, Pred23.cdrom
        |
        |FROM
        |(
        |select T1.Subject, T1.Object as type
        |FROM triples T1
        |where T1.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        |)Pred1
        |
        |
        |LEFT JOIN
        |(
        |select T2.Subject, T2.Object as name
        |FROM triples T2
        |where T2.Predicate="http://xmlns.com/foaf/0.1/name"
        |)Pred2
        |ON Pred1.Subject=Pred2.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T3.Subject, T3.Object as title
        |FROM triples T3
        |where T3.Predicate="http://purl.org/dc/elements/1.1/title"
        |)Pred3
        |ON Pred1.Subject=Pred3.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T4.Subject, T4.Object as issued
        |FROM triples T4
        |where T4.Predicate="http://purl.org/dc/terms/issued"
        |)Pred4
        |ON Pred1.Subject=Pred4.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T5.Subject, T5.Object as creator
        |FROM triples T5
        |where T5.Predicate="http://purl.org/dc/elements/1.1/creator"
        |)Pred5
        |ON Pred1.Subject=Pred5.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T6.Subject, T6.Object as homepage
        |FROM triples T6
        |where T6.Predicate="http://xmlns.com/foaf/0.1/homepage"
        |)Pred6
        |ON Pred1.Subject=Pred6.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T7.Subject, T7.Object as seeAlso
        |FROM triples T7
        |where T7.Predicate="http://www.w3.org/2000/01/rdf-schema#seeAlso"
        |)Pred7
        |ON Pred1.Subject=Pred7.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T8.Subject, T8.Object as booktitle
        |FROM triples T8
        |where T8.Predicate="http://localhost/vocabulary/bench/booktitle"
        |)Pred8
        |ON Pred1.Subject=Pred8.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T9.Subject, T9.Object as partOf
        |FROM triples T9
        |where T9.Predicate="http://purl.org/dc/terms/partOf"
        |)Pred9
        |ON Pred1.Subject=Pred9.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T10.Subject, T10.Object as abstract
        |FROM triples T10
        |where T10.Predicate="http://localhost/vocabulary/bench/abstract"
        |)Pred10
        |ON Pred1.Subject=Pred10.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T11.Subject, T11.Object as pages
        |FROM triples T11
        |where T11.Predicate="http://swrc.ontoware.org/ontology#pages"
        |)Pred11
        |ON Pred1.Subject=Pred11.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T12.Subject, T12.Object as journal
        |FROM triples T12
        |where T12.Predicate="http://swrc.ontoware.org/ontology#journal"
        |)Pred12
        |ON Pred1.Subject=Pred12.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T13.Subject, T13.Object as series
        |FROM triples T13
        |where T13.Predicate="http://swrc.ontoware.org/ontology#series"
        |)Pred13
        |ON Pred1.Subject=Pred13.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T14.Subject, T14.Object as number
        |FROM triples T14
        |where T14.Predicate="http://swrc.ontoware.org/ontology#number"
        |)Pred14
        |ON Pred1.Subject=Pred14.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T15.Subject, T15.Object as note
        |FROM triples T15
        |where T15.Predicate="http://swrc.ontoware.org/ontology#note"
        |)Pred15
        |ON Pred1.Subject=Pred15.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T16.Subject, T16.Object as volume
        |FROM triples T16
        |where T16.Predicate="http://swrc.ontoware.org/ontology#volume"
        |)Pred16
        |ON Pred1.Subject=Pred16.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T17.Subject, T17.Object as subClassOf
        |FROM triples T17
        |where T17.Predicate="http://www.w3.org/2000/01/rdf-schema#subClassOf"
        |)Pred17
        |ON Pred1.Subject=Pred17.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T18.Subject, T18.Object as month
        |FROM triples T18
        |where T18.Predicate="http://swrc.ontoware.org/ontology#month"
        |)Pred18
        |ON Pred1.Subject=Pred18.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T19.Subject, T19.Object as isbn
        |FROM triples T19
        |where T19.Predicate="http://swrc.ontoware.org/ontology#isbn"
        |)Pred19
        |ON Pred1.Subject=Pred19.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T20.Subject, T20.Object as editor
        |FROM triples T20
        |where T20.Predicate="http://swrc.ontoware.org/ontology#editor"
        |)Pred20
        |ON Pred1.Subject=Pred20.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T21.Subject, T21.Object as publisher
        |FROM triples T21
        |where T21.Predicate="http://purl.org/dc/elements/1.1/publisher"
        |)Pred21
        |ON Pred1.Subject=Pred21.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T22.Subject, T22.Object as references
        |FROM triples T22
        |where T22.Predicate="http://purl.org/dc/terms/references"
        |)Pred22
        |ON Pred1.Subject=Pred22.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T23.Subject, T23.Object as cdrom
        |FROM triples T23
        |where T23.Predicate="http://localhost/vocabulary/bench/cdrom"
        |)Pred23
        |ON Pred1.Subject=Pred23.Subject
        |
      """.stripMargin)

 
    wptTable.write.format("csv").option("header", "true").save(s"$path/CSV/WPT/" + "WidePropertyTable.csv")
    println("Saved CSV WPT") 
    wptTable.write.parquet(s"$path/Parquet/WPT/"+ "WidePropertyTable.parquet")
    println("Saved Parquet WPT") 
    wptTable.write.orc(s"$path/ORC/WPT/" + "WidePropertyTable.orc")
    println("Saved ORC WPT") 
    wptTable.write.format("avro").save(s"$path/Avro/WPT/" + "WidePropertyTable.avro")
    println("Saved Avro WPT") 

  }
} 
