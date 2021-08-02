package ee.ut.cs.bigdata.watdiv.querying.avro

import ee.ut.cs.bigdata.watdiv.querying.queries.PTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, FileOutputStream}

object PropertyTables {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Avro PT")
      .getOrCreate()
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/PT/Avro/"

    //read tables from HDFS

    val Product_DF = spark.read.format("avro").load(path + "Product.avro")
    val User_DF = spark.read.format("avro").load(path + "User.avro")
    val Offer_DF = spark.read.format("avro").load(path + "Offer.avro")
    val City_DF = spark.read.format("avro").load(path + "City.avro")
    val Review_DF = spark.read.format("avro").load(path + "Review.avro")
    val Website_DF = spark.read.format("avro").load(path + "Website.avro")
    val Retailer_DF = spark.read.format("avro").load(path + "Retailer.avro")
    val Role_DF = spark.read.format("avro").load(path + "Role.avro")
    val Genre_DF = spark.read.format("avro").load(path + "Genre.avro")
    val Trailer_DF = spark.read.format("avro").load(path + "Trailer.avro")
    val Purchase_DF = spark.read.format("avro").load(path + "Purchase.avro")
    val Language_DF = spark.read.format("avro").load(path + "Language.avro")
    val Likes_DF = spark.read.format("avro").load(path + "Likes.avro")
    val Subscribes_DF = spark.read.format("avro").load(path + "Subscribes.avro")
    val EligibilityRegion_DF = spark.read.format("avro").load(path + "EligibilityRegion.avro")
    val HasGenre_DF = spark.read.format("avro").load(path + "HasGenre.avro")
    val MakesPurchase_DF = spark.read.format("avro").load(path + "MakesPurchase.avro")
    val Tag_DF = spark.read.format("avro").load(path + "Tag.avro")
    val Includes_DF = spark.read.format("avro").load(path + "Includes.avro")
    val Offers_DF = spark.read.format("avro").load(path + "Offers.avro")
    val HasReview_DF = spark.read.format("avro").load(path + "HasReview.avro")
    val FriendOf_DF = spark.read.format("avro").load(path + "FriendOf.avro")
    val Actor_DF = spark.read.format("avro").load(path + "Actor.avro")
    val PurchaseFor_DF = spark.read.format("avro").load(path + "PurchaseFor.avro")

    User_DF.createOrReplaceTempView("User")
    Product_DF.createOrReplaceTempView("Product")
    Offer_DF.createOrReplaceTempView("Offer")
    City_DF.createOrReplaceTempView("City")
    Review_DF.createOrReplaceTempView("Review")
    Website_DF.createOrReplaceTempView("Website")
    Retailer_DF.createOrReplaceTempView("Retailer")
    Role_DF.createOrReplaceTempView("Role")
    Genre_DF.createOrReplaceTempView("Genre")
    Trailer_DF.createOrReplaceTempView("Trailer")
    Purchase_DF.createOrReplaceTempView("Purchase")
    Actor_DF.createOrReplaceTempView("Actor")
    Language_DF.createOrReplaceTempView("Language")
    Likes_DF.createOrReplaceTempView("Likes")
    Subscribes_DF.createOrReplaceTempView("Subscribes")
    EligibilityRegion_DF.createOrReplaceTempView("EligibilityRegion")
    HasGenre_DF.createOrReplaceTempView("HasGenre")
    MakesPurchase_DF.createOrReplaceTempView("MakesPurchase")
    Tag_DF.createOrReplaceTempView("Tag")
    Includes_DF.createOrReplaceTempView("Includes")
    Offers_DF.createOrReplaceTempView("Offers")
    HasReview_DF.createOrReplaceTempView("HasReview")
    FriendOf_DF.createOrReplaceTempView("FriendOf")
    PurchaseFor_DF.createOrReplaceTempView("PurchaseFor")


    //create file to write the query run time results    
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/avro/PT/$ds.txt"), true)

    val queries = List(
      new PTQueries c1,
      new PTQueries c2,
      new PTQueries c3,
      new PTQueries f1,
      new PTQueries f2,
      new PTQueries f3,
      new PTQueries f4,
      new PTQueries f5,
      new PTQueries l1,
      new PTQueries l2,
      new PTQueries l3,
      new PTQueries l4,
      new PTQueries l5,
      new PTQueries s1,
      new PTQueries s2,
      new PTQueries s3,
      new PTQueries s4,
      new PTQueries s5,
      new PTQueries s6,
      new PTQueries s7)

    var count = 1
    for (query <- queries) {
      //run query and calculate the run time
      val startTime = System.nanoTime()
      val df_count = spark.sql(query).count()
      println(df_count)
      //df.take(100).foreach(println)
      val endTime = System.nanoTime()
      val result = (endTime - startTime).toDouble / 1000000000

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
    println("All Queries are Done - Avro - PT!")

  }
}
