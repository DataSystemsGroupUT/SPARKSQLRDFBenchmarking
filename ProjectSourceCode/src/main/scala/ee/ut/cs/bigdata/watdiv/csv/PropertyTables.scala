package ee.ut.cs.bigdata.watdiv.csv

import ee.ut.cs.bigdata.watdiv.queries.PTQueries
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
      .appName("RDFBench CSV PT")
      .getOrCreate()
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/PT/CSV/"

    //read tables from HDFS

    val User_DF = spark.read.option("header", true).csv(path + "User.csv")
    val Product_DF = spark.read.option("header", true).csv(path + "Product.csv")
    val Offer_DF = spark.read.option("header", true).csv(path + "Offer.csv")
    val City_DF = spark.read.option("header", true).csv(path + "City.csv")
    val Review_DF = spark.read.option("header", true).csv(path + "Review.csv")
    val Website_DF = spark.read.option("header", true).csv(path + "Website.csv")
    val Retailer_DF = spark.read.option("header", true).csv(path + "Retailer.csv")
    val Role_DF = spark.read.option("header", true).csv(path + "Role.csv")
    val Genre_DF = spark.read.option("header", true).csv(path + "Genre.csv")
    val Trailer_DF = spark.read.option("header", true).csv(path + "Trailer.csv")
    val Purchase_DF = spark.read.option("header", true).csv(path + "Purchase.csv")
    val Language_DF = spark.read.option("header", true).csv(path + "Language.csv")
    val Likes_DF = spark.read.option("header", true).csv(path + "Likes.csv")
    val Subscribes_DF = spark.read.option("header", true).csv(path + "Subscribes.csv")
    val EligibilityRegion_DF = spark.read.option("header", true).csv(path + "EligibilityRegion.csv")
    val HasGenre_DF = spark.read.option("header", true).csv(path + "HasGenre.csv")
    val MakesPurchase_DF = spark.read.option("header", true).csv(path + "MakesPurchase.csv")
    val Tag_DF = spark.read.option("header", true).csv(path + "Tag.csv")
    val Includes_DF = spark.read.option("header", true).csv(path + "Includes.csv")
    val Offers_DF = spark.read.option("header", true).csv(path + "Offers.csv")
    val HasReview_DF = spark.read.option("header", true).csv(path + "HasReview.csv")
    val FriendOf_DF = spark.read.option("header", true).csv(path + "FriendOf.csv")
    val Actor_DF = spark.read.option("header", true).csv(path + "Actor.csv")
    val PurchaseFor_DF = spark.read.option("header", true).csv(path + "PurchaseFor.csv")

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
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/csv/PT/$ds.txt"), true)

    val queries = List(new PTQueries q1,
      new PTQueries q2,
      new PTQueries q3,
      new PTQueries q4,
      new PTQueries q5,
      new PTQueries q6,
      new PTQueries q7,
      new PTQueries q8,
      new PTQueries q9,
      new PTQueries q10,
      new PTQueries q11,
      new PTQueries q12,
      new PTQueries q13,
      new PTQueries q14,
      new PTQueries q15,
      new PTQueries q16,
      new PTQueries q17,
      new PTQueries q18,
      new PTQueries q19,
      new PTQueries q20)

    var count = 1
    for (query <- queries) {
      //run query and calculate the run time
      val startTime = System.nanoTime()
      val df = spark.sql(query)
      df.take(100).foreach(println)
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
    println("All Queries are Done - CSV - PT!")

  }
}
