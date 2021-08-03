package ee.ut.cs.bigdata.watdiv.querying.avro

import ee.ut.cs.bigdata.watdiv.querying.queries.VTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{File, FileOutputStream}

object VerticalTablesQP {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Avro VT")
      .getOrCreate()
    val ds = args(0) // value = {"100M", "500M, or "1B"}
    var partitionType = args(1) // value = {"Horizontal", "Subject", or "Predicate"}

    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/VP/"


    println("VP Querying!")
    FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(s"$path/$partitionType/Avro")).foreach {
      x =>
        val vpTable = spark.read.format("avro").load(x.getPath().toString)
        vpTable.createOrReplaceTempView(x.getPath().getName().substring(0, x.getPath().getName().lastIndexOf('.')))
    }


    /*

    //read tables from HDFS

    val vpTabl_subscribes = spark.read.format("avro").load(path + "Avro/subscribes.avro")
    val vpTabl_likes = spark.read.format("avro").load(path + "Avro/likes.avro")
    val vpSubscribes = spark.read.format("avro").load(path+ "Avro/" + "subscribes.avro")
    val vpLikes = spark.read.format("avro").load(path+ "Avro/" + "likes.avro")
    val vpCaption = spark.read.format("avro").load(path+ "Avro/" + "caption.avro")
    val vpParentCount = spark.read.format("avro").load(path+ "Avro/" + "parentCountry.avro")
    val vpNationality = spark.read.format("avro").load(path+ "Avro/" + "nationality.avro")
    val vpjobTitle = spark.read.format("avro").load(path+ "Avro/" + "jobTitle.avro")
    val vpText = spark.read.format("avro").load(path+ "Avro/" + "text.avro")
    val vpcontentRating = spark.read.format("avro").load(path+ "Avro/" + "contentRating.avro")
    val vpcontentSize = spark.read.format("avro").load(path+ "Avro/" + "contentSize.avro")
    val vphasReview = spark.read.format("avro").load(path+ "Avro/" + "hasReview.avro")
    val vpTitle = spark.read.format("avro").load(path+ "Avro/" + "og_title.avro")
    val vpRevTitle = spark.read.format("avro").load(path+ "Avro/" + "rev_title.avro")
    val vpreviewer = spark.read.format("avro").load(path+ "Avro/" + "reviewer.avro")
    val vpActor = spark.read.format("avro").load(path+ "Avro/" + "actor.avro")
    val vpLanguage = spark.read.format("avro").load(path+ "Avro/" + "language.avro")
    val vpLocation = spark.read.format("avro").load(path+ "Avro/" + "Location.avro")
    val vpAge = spark.read.format("avro").load(path+ "Avro/" + "age.avro")
    val vpGender = spark.read.format("avro").load(path+ "Avro/" + "gender.avro")
    val vpgivenName = spark.read.format("avro").load(path+ "Avro/" + "givenName.avro")
    val vpfriendOf = spark.read.format("avro").load(path+ "Avro/" + "friendOf.avro")
    val vpLegalName = spark.read.format("avro").load(path+ "Avro/" + "legalName.avro")
    val vpoffers = spark.read.format("avro").load(path+ "Avro/" + "offers.avro")
    val vpeligibleRegion = spark.read.format("avro").load(path+ "Avro/" + "eligibleRegion.avro")
    val vpincludes = spark.read.format("avro").load(path+ "Avro/" + "includes.avro")
    val vphomepage = spark.read.format("avro").load(path+ "Avro/" + "homepage.avro")
    val vpmakesPurchase = spark.read.format("avro").load(path+ "Avro/" + "makesPurchase.avro")
    val vppurchaseFor = spark.read.format("avro").load(path+ "Avro/" + "purchaseFor.avro")
    val vppurchaseDate = spark.read.format("avro").load(path+ "Avro/" + "purchaseDate.avro")
    val vptotalVotes = spark.read.format("avro").load(path+ "Avro/" + "totalVotes.avro")
    val vptag = spark.read.format("avro").load(path+ "Avro/" + "tag.avro")
    val vptype = spark.read.format("avro").load(path+ "Avro/" + "type.avro")
    val vptrailer = spark.read.format("avro").load(path+ "Avro/" + "trailer.avro")
    val vpkeywords = spark.read.format("avro").load(path+ "Avro/" + "keywords.avro")
    val vphasGenre = spark.read.format("avro").load(path+ "Avro/" + "hasGenre.avro")
    val vpdescription = spark.read.format("avro").load(path+ "Avro/" + "description.avro")
    val vpurl = spark.read.format("avro").load(path+ "Avro/" + "url.avro")
    val vphits = spark.read.format("avro").load(path+ "Avro/" + "hits.avro")
    val vpprice = spark.read.format("avro").load(path+ "Avro/" + "price.avro")
    val vpvalidThrough = spark.read.format("avro").load(path+ "Avro/" + "validThrough.avro")
    val vpPricevaliduntil = spark.read.format("avro").load(path+ "Avro/" + "priceValidUntil.avro")
    val vpValidFrom = spark.read.format("avro").load(path+ "Avro/" + "validFrom.avro")
    val vpserialNumber = spark.read.format("avro").load(path+ "Avro/" + "serialNumber.avro")
    val vpeligibleQuantity = spark.read.format("avro").load(path+ "Avro/" + "eligibleQuantity.avro")
    val vppublisher = spark.read.format("avro").load(path+ "Avro/" + "publisher.avro")
    val vpartist = spark.read.format("avro").load(path+ "Avro/" + "artist.avro")
    val vpfamilyName = spark.read.format("avro").load(path+ "Avro/" + "familyName.avro")
    val vpConductor = spark.read.format("avro").load(path+ "Avro/" + "conductor.avro")

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
    vpRevTitle.createOrReplaceTempView("revTitle")
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
    */


    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/avro/VP/$ds.txt"), true)

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


    println("All Queries are Done - Avro - VP!")

  }
}
