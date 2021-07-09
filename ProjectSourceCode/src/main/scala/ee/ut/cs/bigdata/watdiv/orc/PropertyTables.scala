package ee.ut.cs.bigdata.watdiv.orc

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
      .appName("RDFBench ORC PT")
      .getOrCreate()
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/PT/ORC/"

    //read tables from HDFS

    val User_DF = spark.read.format("orc").load(path + "User.orc")
    val Product_DF = spark.read.format("orc").load(path + "Product.orc")
    val Offer_DF = spark.read.format("orc").load(path + "Offer.orc")
    val City_DF = spark.read.format("orc").load(path + "City.orc")
    val Review_DF = spark.read.format("orc").load(path + "Review.orc")
    val Website_DF = spark.read.format("orc").load(path + "Website.orc")
    val Retailer_DF = spark.read.format("orc").load(path + "Retailer.orc")
    val Role_DF = spark.read.format("orc").load(path + "Role.orc")
    val Genre_DF = spark.read.format("orc").load(path + "Genre.orc")
    val Trailer_DF = spark.read.format("orc").load(path + "Trailer.orc")
    val Purchase_DF = spark.read.format("orc").load(path + "Purchase.orc")
    val Language_DF = spark.read.format("orc").load(path + "Language.orc")
    val Likes_DF = spark.read.format("orc").load(path + "Likes.orc")
    val Subscribes_DF = spark.read.format("orc").load(path + "Subscribes.orc")
    val EligibilityRegion_DF = spark.read.format("orc").load(path + "EligibilityRegion.orc")
    val HasGenre_DF = spark.read.format("orc").load(path + "HasGenre.orc")
    val MakesPurchase_DF = spark.read.format("orc").load(path + "MakesPurchase.orc")
    val Tag_DF = spark.read.format("orc").load(path + "Tag.orc")
    val Includes_DF = spark.read.format("orc").load(path + "Includes.orc")
    val Offers_DF = spark.read.format("orc").load(path + "Offers.orc")
    val HasReview_DF = spark.read.format("orc").load(path + "HasReview.orc")
    val FriendOf_DF = spark.read.format("orc").load(path + "FriendOf.orc")
    val Actor_DF = spark.read.format("orc").load(path + "Actor.orc")
    val PurchaseFor_DF = spark.read.format("orc").load(path + "PurchaseFor.orc")

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
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/orc/PT/$ds.txt"), true)

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
    println("All Queries are Done - ORC - PT!")

  }
}
