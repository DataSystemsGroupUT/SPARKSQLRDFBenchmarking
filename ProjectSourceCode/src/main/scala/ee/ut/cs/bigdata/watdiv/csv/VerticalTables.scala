package ee.ut.cs.bigdata.watdiv.csv

import ee.ut.cs.bigdata.watdiv.queries.VTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, FileOutputStream}

object VerticalTables {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench CSV VT")
      .getOrCreate()
    val ds = args(0) // value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/VP/"

    //read tables from HDFS

    val vpTabl_subscribes = spark.read.option("header", true).csv(path + "CSV/subscribes.csv")
    val vpTabl_likes = spark.read.option("header", true).csv(path + "CSV/likes.csv")
    val vpSubscribes = spark.read.option("header", true).csv(path+ "CSV/" + "subscribes.csv")
    val vpLikes = spark.read.option("header", true).csv(path+ "CSV/" + "likes.csv")
    val vpCaption = spark.read.option("header", true).csv(path+ "CSV/" + "caption.csv")
    val vpParentCount = spark.read.option("header", true).csv(path+ "CSV/" + "parentCountry.csv")
    val vpNationality = spark.read.option("header", true).csv(path+ "CSV/" + "nationality.csv")
    val vpjobTitle = spark.read.option("header", true).csv(path+ "CSV/" + "jobTitle.csv")
    val vpText = spark.read.option("header", true).csv(path+ "CSV/" + "text.csv")
    val vpcontentRating = spark.read.option("header", true).csv(path+ "CSV/" + "contentRating.csv")
    val vpcontentSize = spark.read.option("header", true).csv(path+ "CSV/" + "contentSize.csv")
    val vphasReview = spark.read.option("header", true).csv(path+ "CSV/" + "hasReview.csv")
    val vpTitle = spark.read.option("header", true).csv(path+ "CSV/" + "title.csv")
    val vpreviewer = spark.read.option("header", true).csv(path+ "CSV/" + "reviewer.csv")
    val vpActor = spark.read.option("header", true).csv(path+ "CSV/" + "actor.csv")
    val vpLanguage = spark.read.option("header", true).csv(path+ "CSV/" + "language.csv")
    val vpLocation = spark.read.option("header", true).csv(path+ "CSV/" + "Location.csv")
    val vpAge = spark.read.option("header", true).csv(path+ "CSV/" + "age.csv")
    val vpGender = spark.read.option("header", true).csv(path+ "CSV/" + "gender.csv")
    val vpgivenName = spark.read.option("header", true).csv(path+ "CSV/" + "givenName.csv")
    val vpfriendOf = spark.read.option("header", true).csv(path+ "CSV/" + "friendOf.csv")
    val vpLegalName = spark.read.option("header", true).csv(path+ "CSV/" + "legalName.csv")
    val vpoffers = spark.read.option("header", true).csv(path+ "CSV/" + "offers.csv")
    val vpeligibleRegion = spark.read.option("header", true).csv(path+ "CSV/" + "eligibleRegion.csv")
    val vpincludes = spark.read.option("header", true).csv(path+ "CSV/" + "includes.csv")
    val vphomepage = spark.read.option("header", true).csv(path+ "CSV/" + "homepage.csv")
    val vpmakesPurchase = spark.read.option("header", true).csv(path+ "CSV/" + "makesPurchase.csv")
    val vppurchaseFor = spark.read.option("header", true).csv(path+ "CSV/" + "purchaseFor.csv")
    val vppurchaseDate = spark.read.option("header", true).csv(path+ "CSV/" + "purchaseDate.csv")
    val vptotalVotes = spark.read.option("header", true).csv(path+ "CSV/" + "totalVotes.csv")
    val vptag = spark.read.option("header", true).csv(path+ "CSV/" + "tag.csv")
    val vptype = spark.read.option("header", true).csv(path+ "CSV/" + "type.csv")
    val vptrailer = spark.read.option("header", true).csv(path+ "CSV/" + "trailer.csv")
    val vpkeywords = spark.read.option("header", true).csv(path+ "CSV/" + "keywords.csv")
    val vphasGenre = spark.read.option("header", true).csv(path+ "CSV/" + "hasGenre.csv")
    val vpdescription = spark.read.option("header", true).csv(path+ "CSV/" + "description.csv")
    val vpurl = spark.read.option("header", true).csv(path+ "CSV/" + "url.csv")
    val vphits = spark.read.option("header", true).csv(path+ "CSV/" + "hits.csv")
    val vpprice = spark.read.option("header", true).csv(path+ "CSV/" + "price.csv")
    val vpvalidThrough = spark.read.option("header", true).csv(path+ "CSV/" + "validThrough.csv")
    val vpPricevaliduntil = spark.read.option("header", true).csv(path+ "CSV/" + "priceValidUntil.csv")
    val vpValidFrom = spark.read.option("header", true).csv(path+ "CSV/" + "validFrom.csv")
    val vpserialNumber = spark.read.option("header", true).csv(path+ "CSV/" + "serialNumber.csv")
//    val vpeligibleQuantity = spark.read.option("header", true).csv(path+ "CSV/" + "eligibleQuantity.csv")
    val vppublisher = spark.read.option("header", true).csv(path+ "CSV/" + "publisher.csv")
    val vpartist = spark.read.option("header", true).csv(path+ "CSV/" + "artist.csv")
    val vpfamilyName = spark.read.option("header", true).csv(path+ "CSV/" + "familyName.csv")
    val vpConductor = spark.read.option("header", true).csv(path+ "CSV/" + "conductor.csv")

    vpTabl_subscribes.createOrReplaceTempView("Subscribes")
    vpTabl_likes.createOrReplaceTempView("Likes")
    vpConductor.createOrReplaceTempView("conductor")
    vpfamilyName.createOrReplaceTempView("familyName")
    vpartist.createOrReplaceTempView("artist")
    vppublisher.createOrReplaceTempView("publisher")
    vpLikes.createOrReplaceTempView("likes")
    vpSubscribes.createOrReplaceTempView("subscribes")
    vpCaption.createOrReplaceTempView("caption")
    vpParentCount.createOrReplaceTempView("parentcountry")
    vpNationality.createOrReplaceTempView("nationality")
    vpjobTitle.createOrReplaceTempView("jobTitle")
    vpText.createOrReplaceTempView("text")
    vpcontentRating.createOrReplaceTempView("contentRating")
    vphasReview.createOrReplaceTempView("hasReview")
    vpTitle.createOrReplaceTempView("title")
    vpreviewer.createOrReplaceTempView("reviewer")
    vpActor.createOrReplaceTempView("actor")
    vpLanguage.createOrReplaceTempView("language")
    vpLocation.createOrReplaceTempView("location")
    vpAge.createOrReplaceTempView("age")
    vpGender.createOrReplaceTempView("gender")
    vpgivenName.createOrReplaceTempView("givenName")
    vpfriendOf.createOrReplaceTempView("friendOf")
    vpLegalName.createOrReplaceTempView("legalName")
    vpoffers.createOrReplaceTempView("offers")
    vpeligibleRegion.createOrReplaceTempView("eligibleRegion")
    vpincludes.createOrReplaceTempView("includes")
    vphomepage.createOrReplaceTempView("homepage")
    vpmakesPurchase.createOrReplaceTempView("makesPurchase")
    vppurchaseFor.createOrReplaceTempView("purchaseFor")
    vppurchaseDate.createOrReplaceTempView("purchaseDate")
    vptotalVotes.createOrReplaceTempView("totalVotes")
    vptag.createOrReplaceTempView("tag")
    vptype.createOrReplaceTempView("type")
    vptrailer.createOrReplaceTempView("trailer")
    vpkeywords.createOrReplaceTempView("keywords")
    vphasGenre.createOrReplaceTempView("hasGenre")
    vpdescription.createOrReplaceTempView("description")
    vpurl.createOrReplaceTempView("url")
    vphits.createOrReplaceTempView("hits")
    vpcontentSize.createOrReplaceTempView("contentSize")
    vpprice.createOrReplaceTempView("price")
    vpvalidThrough.createOrReplaceTempView("validThrough")
    vpPricevaliduntil.createOrReplaceTempView("priceValidUntil")
    vpValidFrom.createOrReplaceTempView("validFrom")
    vpserialNumber.createOrReplaceTempView("serialNumber")


    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/csv/VP/$ds.txt"), true)

    val queries = List(
      new VTQueries q1,
      new VTQueries q2,
      new VTQueries q3,
      new VTQueries q4,
      new VTQueries q5,
      new VTQueries q6,
      new VTQueries q7,
      new VTQueries q8,
      new VTQueries q9,
      new VTQueries q10,
      new VTQueries q11,
      new VTQueries q12,
      new VTQueries q13,
      new VTQueries q14,
      new VTQueries q15,
      new VTQueries q16,
      new VTQueries q17,
      new VTQueries q18,
      new VTQueries q19,
      new VTQueries q20)

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
    println("All Queries are Done - CSV - VP!")

  }
}
