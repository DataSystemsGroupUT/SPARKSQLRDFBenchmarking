package ee.ut.cs.bigdata.watdiv.partitioning.parquet

import org.apache.log4j.{Level, Logger}
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
      .appName("RDFBench Parquet VT")
      .getOrCreate()

    import spark.implicits._

    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase //value = {"Horizontal", "Subject", or "Predicate"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/VP/"

    println("Start Watdiv VP Partitioning Parquet...")

    //read tables from HDFS
    val vpTabl_subscribes = spark.read.format("parquet").load(path + "VHDFS/Parquet/subscribes.parquet")
    val vpTabl_likes = spark.read.format("parquet").load(path + "VHDFS/Parquet/likes.parquet")
    val vpSubscribes = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "subscribes.parquet")
    val vpLikes = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "likes.parquet")
    val vpCaption = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "caption.parquet")
    val vpParentCount = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "parentCountry.parquet")
    val vpNationality = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "nationality.parquet")
    val vpjobTitle = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "jobTitle.parquet")
    val vpText = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "text.parquet")
    val vpcontentRating = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "contentRating.parquet")
    val vpcontentSize = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "contentSize.parquet")
    val vphasReview = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "hasReview.parquet")
    val vpTitle = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "og_title.parquet")
    val vpRevTitle = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "rev_title.parquet")
    val vpreviewer = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "reviewer.parquet")
    val vpActor = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "actor.parquet")
    val vpLanguage = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "language.parquet")
    val vpLocation = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "Location.parquet")
    val vpAge = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "age.parquet")
    val vpGender = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "gender.parquet")
    val vpgivenName = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "givenName.parquet")
    val vpfriendOf = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "friendOf.parquet")
    val vpLegalName = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "legalName.parquet")
    val vpoffers = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "offers.parquet")
    val vpeligibleRegion = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "eligibleRegion.parquet")
    val vpincludes = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "includes.parquet")
    val vphomepage = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "homepage.parquet")
    val vpmakesPurchase = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "makesPurchase.parquet")
    val vppurchaseFor = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "purchaseFor.parquet")
    val vppurchaseDate = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "purchaseDate.parquet")
    val vptotalVotes = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "totalVotes.parquet")
    val vptag = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "tag.parquet")
    val vptype = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "type.parquet")
    val vptrailer = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "trailer.parquet")
    val vpkeywords = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "keywords.parquet")
    val vphasGenre = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "hasGenre.parquet")
    val vpdescription = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "description.parquet")
    val vpurl = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "url.parquet")
    val vphits = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "hits.parquet")
    val vpprice = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "price.parquet")
    val vpvalidThrough = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "validThrough.parquet")
    val vpPricevaliduntil = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "priceValidUntil.parquet")
    val vpValidFrom = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "validFrom.parquet")
    val vpserialNumber = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "serialNumber.parquet")
    val vpeligibleQuantity = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "eligibleQuantity.parquet")
    val vppublisher = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "publisher.parquet")
    val vpartist = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "artist.parquet")
    val vpfamilyName = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "familyName.parquet")
    val vpConductor = spark.read.format("parquet").load(path + "VHDFS/Parquet/" + "conductor.parquet")


    println("WatDiv VP Tables Read!")

    //partition and save on HDFS
    if (partitionType == "subject") {
      vpTabl_subscribes.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/subscribes.parquet")
      vpTabl_likes.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/likes.parquet")
      vpSubscribes.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "subscribes.parquet")
      vpLikes.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "likes.parquet")
      vpCaption.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "caption.parquet")
      vpParentCount.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "parentCountry.parquet")
      vpNationality.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "nationality.parquet")
      vpjobTitle.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "jobTitle.parquet")
      vpText.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "text.parquet")
      vpcontentRating.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "contentRating.parquet")
      vpcontentSize.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "contentSize.parquet")
      vphasReview.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "hasReview.parquet")
      vpTitle.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "og_title.parquet")
      vpRevTitle.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "rev_title.parquet")
      vpreviewer.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "reviewer.parquet")
      vpActor.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "actor.parquet")
      vpLanguage.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "language.parquet")
      vpLocation.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "Location.parquet")
      vpAge.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "age.parquet")
      vpGender.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "gender.parquet")
      vpgivenName.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "givenName.parquet")
      vpfriendOf.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "friendOf.parquet")
      vpLegalName.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "legalName.parquet")
      vpoffers.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "offers.parquet")
      vpeligibleRegion.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "eligibleRegion.parquet")
      vpincludes.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "includes.parquet")
      vphomepage.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "homepage.parquet")
      vpmakesPurchase.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "makesPurchase.parquet")
      vppurchaseFor.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "purchaseFor.parquet")
      vppurchaseDate.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "purchaseDate.parquet")
      vptotalVotes.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "totalVotes.parquet")
      vptag.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "tag.parquet")
      vptype.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "type.parquet")
      vptrailer.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "trailer.parquet")
      vpkeywords.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "keywords.parquet")
      vphasGenre.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "hasGenre.parquet")
      vpdescription.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "description.parquet")
      vpurl.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "url.parquet")
      vphits.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "hits.parquet")
      vpprice.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "price.parquet")
      vpvalidThrough.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "validThrough.parquet")
      vpPricevaliduntil.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "priceValidUntil.parquet")
      vpValidFrom.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "validFrom.parquet")
      vpserialNumber.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "serialNumber.parquet")
      vpeligibleQuantity.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "eligibleQuantity.parquet")
      vppublisher.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "publisher.parquet")
      vpartist.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "artist.parquet")
      vpfamilyName.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "familyName.parquet")
      vpConductor.repartition($"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/" + "conductor.parquet")

      println("Parquet VT partitioned and saved! Subject based Partitioning!")


    }

    else if (partitionType == "horizontal") {
      vpTabl_subscribes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/subscribes.parquet")

      vpTabl_likes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/likes.parquet")
      vpSubscribes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "subscribes.parquet")
      vpLikes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "likes.parquet")
      vpCaption.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "caption.parquet")
      vpParentCount.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "parentCountry.parquet")
      vpNationality.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "nationality.parquet")
      vpjobTitle.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "jobTitle.parquet")
      vpText.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "text.parquet")
      vpcontentRating.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "contentRating.parquet")
      vpcontentSize.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "contentSize.parquet")
      vphasReview.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "hasReview.parquet")
      vpTitle.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "og_title.parquet")
      vpRevTitle.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "rev_title.parquet")
      vpreviewer.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "reviewer.parquet")
      vpActor.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "actor.parquet")
      vpLanguage.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "language.parquet")
      vpLocation.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "Location.parquet")
      vpAge.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "age.parquet")
      vpGender.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "gender.parquet")
      vpgivenName.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "givenName.parquet")
      vpfriendOf.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "friendOf.parquet")
      vpLegalName.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "legalName.parquet")
      vpoffers.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "offers.parquet")
      vpeligibleRegion.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "eligibleRegion.parquet")
      vpincludes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "includes.parquet")
      vphomepage.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "homepage.parquet")
      vpmakesPurchase.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "makesPurchase.parquet")
      vppurchaseFor.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "purchaseFor.parquet")
      vppurchaseDate.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "purchaseDate.parquet")
      vptotalVotes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "totalVotes.parquet")
      vptag.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "tag.parquet")
      vptype.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "type.parquet")
      vptrailer.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "trailer.parquet")
      vpkeywords.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "keywords.parquet")
      vphasGenre.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "hasGenre.parquet")
      vpdescription.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "description.parquet")
      vpurl.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "url.parquet")
      vphits.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "hits.parquet")
      vpprice.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "price.parquet")
      vpvalidThrough.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "validThrough.parquet")
      vpPricevaliduntil.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "priceValidUntil.parquet")
      vpValidFrom.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "validFrom.parquet")
      vpserialNumber.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "serialNumber.parquet")
      vpeligibleQuantity.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "eligibleQuantity.parquet")
      vppublisher.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "publisher.parquet")
      vpartist.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "artist.parquet")
      vpfamilyName.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "familyName.parquet")
      vpConductor.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/" + "conductor.parquet")


      println("Parquet VT partitioned and saved! Horizontal partitioning!")
    }


  }
}
