package ee.ut.cs.bigdata.watdiv.partitioning.orc

import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.sp2bench.queries.VTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object VerticalTablesPartition {
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

    import spark.implicits._

    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase //value = {"Horizontal", "Subject", or "Predicate"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/VP/"

    println("Start Watdiv VP Partitioning ORC...")

    //read tables from HDFS

    val vpTabl_subscribes = spark.read.format("orc").load(path + "VHDFS/ORC/subscribes.orc")
    val vpTabl_likes = spark.read.format("orc").load(path + "VHDFS/ORC/likes.orc")
    val vpSubscribes = spark.read.format("orc").load(path + "VHDFS/ORC/" + "subscribes.orc")
    val vpLikes = spark.read.format("orc").load(path + "VHDFS/ORC/" + "likes.orc")
    val vpCaption = spark.read.format("orc").load(path + "VHDFS/ORC/" + "caption.orc")
    val vpParentCount = spark.read.format("orc").load(path + "VHDFS/ORC/" + "parentCountry.orc")
    val vpNationality = spark.read.format("orc").load(path + "VHDFS/ORC/" + "nationality.orc")
    val vpjobTitle = spark.read.format("orc").load(path + "VHDFS/ORC/" + "jobTitle.orc")
    val vpText = spark.read.format("orc").load(path + "VHDFS/ORC/" + "text.orc")
    val vpcontentRating = spark.read.format("orc").load(path + "VHDFS/ORC/" + "contentRating.orc")
    val vpcontentSize = spark.read.format("orc").load(path + "VHDFS/ORC/" + "contentSize.orc")
    val vphasReview = spark.read.format("orc").load(path + "VHDFS/ORC/" + "hasReview.orc")
    val vpTitle = spark.read.format("orc").load(path + "VHDFS/ORC/" + "og_title.orc")
    val vpRevTitle = spark.read.format("orc").load(path + "VHDFS/ORC/" + "rev_title.orc")
    val vpreviewer = spark.read.format("orc").load(path + "VHDFS/ORC/" + "reviewer.orc")
    val vpActor = spark.read.format("orc").load(path + "VHDFS/ORC/" + "actor.orc")
    val vpLanguage = spark.read.format("orc").load(path + "VHDFS/ORC/" + "language.orc")
    val vpLocation = spark.read.format("orc").load(path + "VHDFS/ORC/" + "Location.orc")
    val vpAge = spark.read.format("orc").load(path + "VHDFS/ORC/" + "age.orc")
    val vpGender = spark.read.format("orc").load(path + "VHDFS/ORC/" + "gender.orc")
    val vpgivenName = spark.read.format("orc").load(path + "VHDFS/ORC/" + "givenName.orc")
    val vpfriendOf = spark.read.format("orc").load(path + "VHDFS/ORC/" + "friendOf.orc")
    val vpLegalName = spark.read.format("orc").load(path + "VHDFS/ORC/" + "legalName.orc")
    val vpoffers = spark.read.format("orc").load(path + "VHDFS/ORC/" + "offers.orc")
    val vpeligibleRegion = spark.read.format("orc").load(path + "VHDFS/ORC/" + "eligibleRegion.orc")
    val vpincludes = spark.read.format("orc").load(path + "VHDFS/ORC/" + "includes.orc")
    val vphomepage = spark.read.format("orc").load(path + "VHDFS/ORC/" + "homepage.orc")
    val vpmakesPurchase = spark.read.format("orc").load(path + "VHDFS/ORC/" + "makesPurchase.orc")
    val vppurchaseFor = spark.read.format("orc").load(path + "VHDFS/ORC/" + "purchaseFor.orc")
    val vppurchaseDate = spark.read.format("orc").load(path + "VHDFS/ORC/" + "purchaseDate.orc")
    val vptotalVotes = spark.read.format("orc").load(path + "VHDFS/ORC/" + "totalVotes.orc")
    val vptag = spark.read.format("orc").load(path + "VHDFS/ORC/" + "tag.orc")
    val vptype = spark.read.format("orc").load(path + "VHDFS/ORC/" + "type.orc")
    val vptrailer = spark.read.format("orc").load(path + "VHDFS/ORC/" + "trailer.orc")
    val vpkeywords = spark.read.format("orc").load(path + "VHDFS/ORC/" + "keywords.orc")
    val vphasGenre = spark.read.format("orc").load(path + "VHDFS/ORC/" + "hasGenre.orc")
    val vpdescription = spark.read.format("orc").load(path + "VHDFS/ORC/" + "description.orc")
    val vpurl = spark.read.format("orc").load(path + "VHDFS/ORC/" + "url.orc")
    val vphits = spark.read.format("orc").load(path + "VHDFS/ORC/" + "hits.orc")
    val vpprice = spark.read.format("orc").load(path + "VHDFS/ORC/" + "price.orc")
    val vpvalidThrough = spark.read.format("orc").load(path + "VHDFS/ORC/" + "validThrough.orc")
    val vpPricevaliduntil = spark.read.format("orc").load(path + "VHDFS/ORC/" + "priceValidUntil.orc")
    val vpValidFrom = spark.read.format("orc").load(path + "VHDFS/ORC/" + "validFrom.orc")
    val vpserialNumber = spark.read.format("orc").load(path + "VHDFS/ORC/" + "serialNumber.orc")
    val vpeligibleQuantity = spark.read.format("orc").load(path + "VHDFS/ORC/" + "eligibleQuantity.orc")
    val vppublisher = spark.read.format("orc").load(path + "VHDFS/ORC/" + "publisher.orc")
    val vpartist = spark.read.format("orc").load(path + "VHDFS/ORC/" + "artist.orc")
    val vpfamilyName = spark.read.format("orc").load(path + "VHDFS/ORC/" + "familyName.orc")
    val vpConductor = spark.read.format("orc").load(path + "VHDFS/ORC/" + "conductor.orc")

    println("WatDiv VP Tables Read!")

    //partition and save on HDFS
    if (partitionType == "subject") {

      vpTabl_subscribes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/subscribes.orc")
      vpTabl_likes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/likes.orc")
      vpSubscribes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "subscribes.orc")
      vpLikes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "likes.orc")
      vpCaption.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "caption.orc")
      vpParentCount.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "parentCountry.orc")
      vpNationality.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "nationality.orc")
      vpjobTitle.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "jobTitle.orc")
      vpText.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "text.orc")
      vpcontentRating.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "contentRating.orc")
      vpcontentSize.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "contentSize.orc")
      vphasReview.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "hasReview.orc")
      vpTitle.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "og_title.orc")
      vpRevTitle.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "rev_title.orc")
      vpreviewer.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "reviewer.orc")
      vpActor.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "actor.orc")
      vpLanguage.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "language.orc")
      vpLocation.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "Location.orc")
      vpAge.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "age.orc")
      vpGender.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "gender.orc")
      vpgivenName.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "givenName.orc")
      vpfriendOf.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "friendOf.orc")
      vpLegalName.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "legalName.orc")
      vpoffers.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "offers.orc")
      vpeligibleRegion.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "eligibleRegion.orc")
      vpincludes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "includes.orc")
      vphomepage.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "homepage.orc")
      vpmakesPurchase.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "makesPurchase.orc")
      vppurchaseFor.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "purchaseFor.orc")
      vppurchaseDate.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "purchaseDate.orc")
      vptotalVotes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "totalVotes.orc")
      vptag.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "tag.orc")
      vptype.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "type.orc")
      vptrailer.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "trailer.orc")
      vpkeywords.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "keywords.orc")
      vphasGenre.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "hasGenre.orc")
      vpdescription.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "description.orc")
      vpurl.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "url.orc")
      vphits.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "hits.orc")
      vpprice.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "price.orc")
      vpvalidThrough.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "validThrough.orc")
      vpPricevaliduntil.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "priceValidUntil.orc")
      vpValidFrom.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "validFrom.orc")
      vpserialNumber.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "serialNumber.orc")
      vpeligibleQuantity.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "eligibleQuantity.orc")
      vppublisher.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "publisher.orc")
      vpartist.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "artist.orc")
      vpfamilyName.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "familyName.orc")
      vpConductor.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/" + "conductor.orc")

      println("ORC VT partitioned and saved! Subject based Partitioning")
    }

    else if (partitionType == "horizontal") {

      vpTabl_subscribes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/subscribes.orc")
      vpTabl_likes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/likes.orc")
      vpSubscribes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "subscribes.orc")
      vpLikes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "likes.orc")
      vpCaption.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "caption.orc")
      vpParentCount.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "parentCountry.orc")
      vpNationality.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "nationality.orc")
      vpjobTitle.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "jobTitle.orc")
      vpText.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "text.orc")
      vpcontentRating.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "contentRating.orc")
      vpcontentSize.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "contentSize.orc")
      vphasReview.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "hasReview.orc")
      vpTitle.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "og_title.orc")
      vpRevTitle.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "rev_title.orc")
      vpreviewer.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "reviewer.orc")
      vpActor.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "actor.orc")
      vpLanguage.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "language.orc")
      vpLocation.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "Location.orc")
      vpAge.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "age.orc")
      vpGender.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "gender.orc")
      vpgivenName.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "givenName.orc")
      vpfriendOf.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "friendOf.orc")
      vpLegalName.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "legalName.orc")
      vpoffers.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "offers.orc")
      vpeligibleRegion.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "eligibleRegion.orc")
      vpincludes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "includes.orc")
      vphomepage.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "homepage.orc")
      vpmakesPurchase.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "makesPurchase.orc")
      vppurchaseFor.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "purchaseFor.orc")
      vppurchaseDate.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "purchaseDate.orc")
      vptotalVotes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "totalVotes.orc")
      vptag.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "tag.orc")
      vptype.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "type.orc")
      vptrailer.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "trailer.orc")
      vpkeywords.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "keywords.orc")
      vphasGenre.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "hasGenre.orc")
      vpdescription.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "description.orc")
      vpurl.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "url.orc")
      vphits.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "hits.orc")
      vpprice.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "price.orc")
      vpvalidThrough.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "validThrough.orc")
      vpPricevaliduntil.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "priceValidUntil.orc")
      vpValidFrom.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "validFrom.orc")
      vpserialNumber.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "serialNumber.orc")
      vpeligibleQuantity.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "eligibleQuantity.orc")
      vppublisher.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "publisher.orc")
      vpartist.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "artist.orc")
      vpfamilyName.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "familyName.orc")
      vpConductor.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/" + "conductor.orc")

      println("ORC VT partitioned and saved! Horizontal partitioning")

    }

  }
}
