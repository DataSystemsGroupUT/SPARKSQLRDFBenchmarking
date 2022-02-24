package ee.ut.cs.bigdata.watdiv.querying.parquet

import ee.ut.cs.bigdata.watdiv.querying.queries.VTQueries
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
      .appName("RDFBench Parquet VT")
      .getOrCreate()
    val ds = args(0) // value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/VP/VHDFS/"

    //read tables from HDFS

    val vpTabl_subscribes = spark.read.format("parquet").load(path + "Parquet/subscribes.parquet")
    val vpTabl_likes = spark.read.format("parquet").load(path + "Parquet/likes.parquet")
    val vpSubscribes = spark.read.format("parquet").load(path+ "Parquet/" + "subscribes.parquet")
    val vpLikes = spark.read.format("parquet").load(path+ "Parquet/" + "likes.parquet")
    val vpCaption = spark.read.format("parquet").load(path+ "Parquet/" + "caption.parquet")
    val vpParentCount = spark.read.format("parquet").load(path+ "Parquet/" + "parentCountry.parquet")
    val vpNationality = spark.read.format("parquet").load(path+ "Parquet/" + "nationality.parquet")
    val vpjobTitle = spark.read.format("parquet").load(path+ "Parquet/" + "jobTitle.parquet")
    val vpText = spark.read.format("parquet").load(path+ "Parquet/" + "sorg_text.parquet")
    val vpcontentRating = spark.read.format("parquet").load(path+ "Parquet/" + "contentRating.parquet")
    val vpcontentSize = spark.read.format("parquet").load(path+ "Parquet/" + "contentSize.parquet")
    val vphasReview = spark.read.format("parquet").load(path+ "Parquet/" + "hasReview.parquet")
    val vpTitle = spark.read.format("parquet").load(path+ "Parquet/" + "og_title.parquet")
    val vpRevTitle = spark.read.format("parquet").load(path+ "Parquet/" + "rev_title.parquet")
    val vpreviewer = spark.read.format("parquet").load(path+ "Parquet/" + "reviewer.parquet")
    val vpActor = spark.read.format("parquet").load(path+ "Parquet/" + "actor.parquet")
    val vpLanguage = spark.read.format("parquet").load(path+ "Parquet/" + "language.parquet")
    val vpLocation = spark.read.format("parquet").load(path+ "Parquet/" + "Location.parquet")
    val vpAge = spark.read.format("parquet").load(path+ "Parquet/" + "age.parquet")
    val vpGender = spark.read.format("parquet").load(path+ "Parquet/" + "gender.parquet")
    val vpgivenName = spark.read.format("parquet").load(path+ "Parquet/" + "givenName.parquet")
    val vpfriendOf = spark.read.format("parquet").load(path+ "Parquet/" + "friendOf.parquet")
    val vpLegalName = spark.read.format("parquet").load(path+ "Parquet/" + "legalName.parquet")
    val vpoffers = spark.read.format("parquet").load(path+ "Parquet/" + "offers.parquet")
    val vpeligibleRegion = spark.read.format("parquet").load(path+ "Parquet/" + "eligibleRegion.parquet")
    val vpincludes = spark.read.format("parquet").load(path+ "Parquet/" + "includes.parquet")
    val vphomepage = spark.read.format("parquet").load(path+ "Parquet/" + "homepage.parquet")
    val vpmakesPurchase = spark.read.format("parquet").load(path+ "Parquet/" + "makesPurchase.parquet")
    val vppurchaseFor = spark.read.format("parquet").load(path+ "Parquet/" + "purchaseFor.parquet")
    val vppurchaseDate = spark.read.format("parquet").load(path+ "Parquet/" + "purchaseDate.parquet")
    val vptotalVotes = spark.read.format("parquet").load(path+ "Parquet/" + "totalVotes.parquet")
    val vptag = spark.read.format("parquet").load(path+ "Parquet/" + "tag.parquet")
    val vptype = spark.read.format("parquet").load(path+ "Parquet/" + "type.parquet")
    val vptrailer = spark.read.format("parquet").load(path+ "Parquet/" + "trailer.parquet")
    val vpkeywords = spark.read.format("parquet").load(path+ "Parquet/" + "keywords.parquet")
    val vphasGenre = spark.read.format("parquet").load(path+ "Parquet/" + "hasGenre.parquet")
    val vpdescription = spark.read.format("parquet").load(path+ "Parquet/" + "sorg_description.parquet")
    val vpurl = spark.read.format("parquet").load(path+ "Parquet/" + "url.parquet")
    val vphits = spark.read.format("parquet").load(path+ "Parquet/" + "hits.parquet")
    val vpprice = spark.read.format("parquet").load(path+ "Parquet/" + "price.parquet")
    val vpvalidThrough = spark.read.format("parquet").load(path+ "Parquet/" + "validThrough.parquet")
    val vpPricevaliduntil = spark.read.format("parquet").load(path+ "Parquet/" + "priceValidUntil.parquet")
    val vpValidFrom = spark.read.format("parquet").load(path+ "Parquet/" + "validFrom.parquet")
    val vpserialNumber = spark.read.format("parquet").load(path+ "Parquet/" + "serialNumber.parquet")
    val vpeligibleQuantity = spark.read.format("parquet").load(path+ "Parquet/" + "eligibleQuantity.parquet")
    val vppublisher = spark.read.format("parquet").load(path+ "Parquet/" + "publisher.parquet")
    val vpartist = spark.read.format("parquet").load(path+ "Parquet/" + "artist.parquet")
    val vpfamilyName = spark.read.format("parquet").load(path+ "Parquet/" + "familyName.parquet")
    val vpConductor = spark.read.format("parquet").load(path+ "Parquet/" + "conductor.parquet")

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
    vpRevTitle.createOrReplaceTempView("rev_Title")
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
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/parquet/VP/$ds.txt"), true)

    val queries = List(
      new VTQueries c1,
      new VTQueries c2,
      new VTQueries c3,
      new VTQueries f1,
      new VTQueries f2,
      new VTQueries f3,
      new VTQueries f4,
      new VTQueries f5,
      new VTQueries l1,
      new VTQueries l2,
      new VTQueries l3,
      new VTQueries l4,
      new VTQueries l5,
      new VTQueries s1,
      new VTQueries s2,
      new VTQueries s3,
      new VTQueries s4,
      new VTQueries s5,
      new VTQueries s6,
      new VTQueries s7)

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
    println("All Queries are Done - Parquet - VP!")

  }
}
