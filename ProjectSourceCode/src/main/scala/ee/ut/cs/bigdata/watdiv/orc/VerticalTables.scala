package ee.ut.cs.bigdata.watdiv.orc

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
      .appName("RDFBench ORC VT")
      .getOrCreate()
    val ds = args(0) // value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/VP/"

    //read tables from HDFS

    val vpTabl_subscribes = spark.read.format("orc").load(path + "ORC/subscribes.orc")
    val vpTabl_likes = spark.read.format("orc").load(path + "ORC/likes.orc")
    val vpSubscribes = spark.read.format("orc").load(path+ "ORC/" + "subscribes.orc")
    val vpLikes = spark.read.format("orc").load(path+ "ORC/" + "likes.orc")
    val vpCaption = spark.read.format("orc").load(path+ "ORC/" + "caption.orc")
    val vpParentCount = spark.read.format("orc").load(path+ "ORC/" + "parentCountry.orc")
    val vpNationality = spark.read.format("orc").load(path+ "ORC/" + "nationality.orc")
    val vpjobTitle = spark.read.format("orc").load(path+ "ORC/" + "jobTitle.orc")
    val vpText = spark.read.format("orc").load(path+ "ORC/" + "text.orc")
    val vpcontentRating = spark.read.format("orc").load(path+ "ORC/" + "contentRating.orc")
    val vpcontentSize = spark.read.format("orc").load(path+ "ORC/" + "contentSize.orc")
    val vphasReview = spark.read.format("orc").load(path+ "ORC/" + "hasReview.orc")
    val vpTitle = spark.read.format("orc").load(path+ "ORC/" + "title.orc")
    val vpreviewer = spark.read.format("orc").load(path+ "ORC/" + "reviewer.orc")
    val vpActor = spark.read.format("orc").load(path+ "ORC/" + "actor.orc")
    val vpLanguage = spark.read.format("orc").load(path+ "ORC/" + "language.orc")
    val vpLocation = spark.read.format("orc").load(path+ "ORC/" + "Location.orc")
    val vpAge = spark.read.format("orc").load(path+ "ORC/" + "age.orc")
    val vpGender = spark.read.format("orc").load(path+ "ORC/" + "gender.orc")
    val vpgivenName = spark.read.format("orc").load(path+ "ORC/" + "givenName.orc")
    val vpfriendOf = spark.read.format("orc").load(path+ "ORC/" + "friendOf.orc")
    val vpLegalName = spark.read.format("orc").load(path+ "ORC/" + "legalName.orc")
    val vpoffers = spark.read.format("orc").load(path+ "ORC/" + "offers.orc")
    val vpeligibleRegion = spark.read.format("orc").load(path+ "ORC/" + "eligibleRegion.orc")
    val vpincludes = spark.read.format("orc").load(path+ "ORC/" + "includes.orc")
    val vphomepage = spark.read.format("orc").load(path+ "ORC/" + "homepage.orc")
    val vpmakesPurchase = spark.read.format("orc").load(path+ "ORC/" + "makesPurchase.orc")
    val vppurchaseFor = spark.read.format("orc").load(path+ "ORC/" + "purchaseFor.orc")
    val vppurchaseDate = spark.read.format("orc").load(path+ "ORC/" + "purchaseDate.orc")
    val vptotalVotes = spark.read.format("orc").load(path+ "ORC/" + "totalVotes.orc")
    val vptag = spark.read.format("orc").load(path+ "ORC/" + "tag.orc")
    val vptype = spark.read.format("orc").load(path+ "ORC/" + "type.orc")
    val vptrailer = spark.read.format("orc").load(path+ "ORC/" + "trailer.orc")
    val vpkeywords = spark.read.format("orc").load(path+ "ORC/" + "keywords.orc")
    val vphasGenre = spark.read.format("orc").load(path+ "ORC/" + "hasGenre.orc")
    val vpdescription = spark.read.format("orc").load(path+ "ORC/" + "description.orc")
    val vpurl = spark.read.format("orc").load(path+ "ORC/" + "url.orc")
    val vphits = spark.read.format("orc").load(path+ "ORC/" + "hits.orc")
    val vpprice = spark.read.format("orc").load(path+ "ORC/" + "price.orc")
    val vpvalidThrough = spark.read.format("orc").load(path+ "ORC/" + "validThrough.orc")
    val vpPricevaliduntil = spark.read.format("orc").load(path+ "ORC/" + "priceValidUntil.orc")
    val vpValidFrom = spark.read.format("orc").load(path+ "ORC/" + "validFrom.orc")
    val vpserialNumber = spark.read.format("orc").load(path+ "ORC/" + "serialNumber.orc")
    val vpeligibleQuantity = spark.read.format("orc").load(path+ "ORC/" + "eligibleQuantity.orc")
    val vppublisher = spark.read.format("orc").load(path+ "ORC/" + "publisher.orc")
    val vpartist = spark.read.format("orc").load(path+ "ORC/" + "artist.orc")
    val vpfamilyName = spark.read.format("orc").load(path+ "ORC/" + "familyName.orc")
    val vpConductor = spark.read.format("orc").load(path+ "ORC/" + "conductor.orc")

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
    vpeligibleQuantity.createOrReplaceTempView("eligibleQuantity")


    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/orc/VP/$ds.txt"), true)

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
    println("All Queries are Done - ORC - VP!")

  }
}
