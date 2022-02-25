package ee.ut.cs.bigdata.watdiv.partitioning.avro

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
      .appName("RDFBench Avro VT")
      .getOrCreate()

    import spark.implicits._

    val ds = args(0) // value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase // horizontal, predicate or subject
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/VP/"


    println("Start Watdiv VP Partitioning Avro...")
    //read tables from HDFS

    val vpTabl_subscribes = spark.read.format("avro").load(path + "VHDFS/Avro/subscribes.avro")
    val vpTabl_likes = spark.read.format("avro").load(path + "VHDFS/Avro/likes.avro")
    val vpSubscribes = spark.read.format("avro").load(path + "VHDFS/Avro/" + "subscribes.avro")
    val vpLikes = spark.read.format("avro").load(path + "VHDFS/Avro/" + "likes.avro")
    val vpCaption = spark.read.format("avro").load(path + "VHDFS/Avro/" + "caption.avro")
    val vpParentCount = spark.read.format("avro").load(path + "VHDFS/Avro/" + "parentCountry.avro")
    val vpNationality = spark.read.format("avro").load(path + "VHDFS/Avro/" + "nationality.avro")
    val vpjobTitle = spark.read.format("avro").load(path + "VHDFS/Avro/" + "jobTitle.avro")
    val vpText = spark.read.format("avro").load(path + "VHDFS/Avro/" + "sorg_text.avro")
    val vpcontentRating = spark.read.format("avro").load(path + "VHDFS/Avro/" + "contentRating.avro")
    val vpcontentSize = spark.read.format("avro").load(path + "VHDFS/Avro/" + "contentSize.avro")
    val vphasReview = spark.read.format("avro").load(path + "VHDFS/Avro/" + "hasReview.avro")
    val vpTitle = spark.read.format("avro").load(path + "VHDFS/Avro/" + "og_title.avro")
    val vpRevTitle = spark.read.format("avro").load(path + "VHDFS/Avro/" + "rev_title.avro")
    val vpreviewer = spark.read.format("avro").load(path + "VHDFS/Avro/" + "reviewer.avro")
    val vpActor = spark.read.format("avro").load(path + "VHDFS/Avro/" + "actor.avro")
    val vpLanguage = spark.read.format("avro").load(path + "VHDFS/Avro/" + "language.avro")
    val vpLocation = spark.read.format("avro").load(path + "VHDFS/Avro/" + "Location.avro")
    val vpAge = spark.read.format("avro").load(path + "VHDFS/Avro/" + "age.avro")
    val vpGender = spark.read.format("avro").load(path + "VHDFS/Avro/" + "gender.avro")
    val vpgivenName = spark.read.format("avro").load(path + "VHDFS/Avro/" + "givenName.avro")
    val vpfriendOf = spark.read.format("avro").load(path + "VHDFS/Avro/" + "friendOf.avro")
    val vpLegalName = spark.read.format("avro").load(path + "VHDFS/Avro/" + "legalName.avro")
    val vpoffers = spark.read.format("avro").load(path + "VHDFS/Avro/" + "offers.avro")
    val vpeligibleRegion = spark.read.format("avro").load(path + "VHDFS/Avro/" + "eligibleRegion.avro")
    val vpincludes = spark.read.format("avro").load(path + "VHDFS/Avro/" + "includes.avro")
    val vphomepage = spark.read.format("avro").load(path + "VHDFS/Avro/" + "homepage.avro")
    val vpmakesPurchase = spark.read.format("avro").load(path + "VHDFS/Avro/" + "makesPurchase.avro")
    val vppurchaseFor = spark.read.format("avro").load(path + "VHDFS/Avro/" + "purchaseFor.avro")
    val vppurchaseDate = spark.read.format("avro").load(path + "VHDFS/Avro/" + "purchaseDate.avro")
    val vptotalVotes = spark.read.format("avro").load(path + "VHDFS/Avro/" + "totalVotes.avro")
    val vptag = spark.read.format("avro").load(path + "VHDFS/Avro/" + "tag.avro")
    val vptype = spark.read.format("avro").load(path + "VHDFS/Avro/" + "type.avro")
    val vptrailer = spark.read.format("avro").load(path + "VHDFS/Avro/" + "trailer.avro")
    val vpkeywords = spark.read.format("avro").load(path + "VHDFS/Avro/" + "keywords.avro")
    val vphasGenre = spark.read.format("avro").load(path + "VHDFS/Avro/" + "hasGenre.avro")
    val vpdescription = spark.read.format("avro").load(path + "VHDFS/Avro/" + "sorg_description.avro")
    val vpurl = spark.read.format("avro").load(path + "VHDFS/Avro/" + "url.avro")
    val vphits = spark.read.format("avro").load(path + "VHDFS/Avro/" + "hits.avro")
    val vpprice = spark.read.format("avro").load(path + "VHDFS/Avro/" + "price.avro")
    val vpvalidThrough = spark.read.format("avro").load(path + "VHDFS/Avro/" + "validThrough.avro")
    val vpPricevaliduntil = spark.read.format("avro").load(path + "VHDFS/Avro/" + "priceValidUntil.avro")
    val vpValidFrom = spark.read.format("avro").load(path + "VHDFS/Avro/" + "validFrom.avro")
    val vpserialNumber = spark.read.format("avro").load(path + "VHDFS/Avro/" + "serialNumber.avro")
    val vpeligibleQuantity = spark.read.format("avro").load(path + "VHDFS/Avro/" + "eligibleQuantity.avro")
    val vppublisher = spark.read.format("avro").load(path + "VHDFS/Avro/" + "publisher.avro")
    val vpartist = spark.read.format("avro").load(path + "VHDFS/Avro/" + "artist.avro")
    val vpfamilyName = spark.read.format("avro").load(path + "VHDFS/Avro/" + "familyName.avro")
    val vpConductor = spark.read.format("avro").load(path + "VHDFS/Avro/" + "conductor.avro")


    println("WatDiv VP Tables Read!")

    //partition and save on HDFS
    if (partitionType == "subject") {

      vpTabl_subscribes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/subscribes.avro")
      vpTabl_likes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/likes.avro")
      vpSubscribes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "subscribes.avro")
      vpLikes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "likes.avro")
      vpCaption.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "caption.avro")
      vpParentCount.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "parentCountry.avro")
      vpNationality.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "nationality.avro")
      vpjobTitle.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "jobTitle.avro")
      vpText.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "text.avro")
      vpcontentRating.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "contentRating.avro")
      vpcontentSize.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "contentSize.avro")
      vphasReview.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "hasReview.avro")
      vpTitle.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "og_title.avro")
      vpRevTitle.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "rev_title.avro")
      vpreviewer.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "reviewer.avro")
      vpActor.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "actor.avro")
      vpLanguage.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "language.avro")
      vpLocation.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "Location.avro")
      vpAge.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "age.avro")
      vpGender.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "gender.avro")
      vpgivenName.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "givenName.avro")
      vpfriendOf.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "friendOf.avro")
      vpLegalName.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "legalName.avro")
      vpoffers.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "offers.avro")
      vpeligibleRegion.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "eligibleRegion.avro")
      vpincludes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "includes.avro")
      vphomepage.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "homepage.avro")
      vpmakesPurchase.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "makesPurchase.avro")
      vppurchaseFor.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "purchaseFor.avro")
      vppurchaseDate.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "purchaseDate.avro")
      vptotalVotes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "totalVotes.avro")
      vptag.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "tag.avro")
      vptype.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "type.avro")
      vptrailer.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "trailer.avro")
      vpkeywords.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "keywords.avro")
      vphasGenre.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "hasGenre.avro")
      vpdescription.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "description.avro")
      vpurl.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "url.avro")
      vphits.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "hits.avro")
      vpprice.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "price.avro")
      vpvalidThrough.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "validThrough.avro")
      vpPricevaliduntil.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "priceValidUntil.avro")
      vpValidFrom.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "validFrom.avro")
      vpserialNumber.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "serialNumber.avro")
      vpeligibleQuantity.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "eligibleQuantity.avro")
      vppublisher.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "publisher.avro")
      vpartist.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "artist.avro")
      vpfamilyName.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "familyName.avro")
      vpConductor.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/" + "conductor.avro")


      println("Avro VT partitioned and saved! Subject based partitioning!")
    }


    else if (partitionType == "horizontal") {
      vpTabl_subscribes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/subscribes.avro")
      vpTabl_likes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/likes.avro")
      vpSubscribes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "subscribes.avro")
      vpLikes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "likes.avro")
      vpCaption.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "caption.avro")
      vpParentCount.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "parentCountry.avro")
      vpNationality.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "nationality.avro")
      vpjobTitle.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "jobTitle.avro")
      vpText.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "text.avro")
      vpcontentRating.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "contentRating.avro")
      vpcontentSize.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "contentSize.avro")
      vphasReview.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "hasReview.avro")
      vpTitle.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "og_title.avro")
      vpRevTitle.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "rev_title.avro")
      vpreviewer.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "reviewer.avro")
      vpActor.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "actor.avro")
      vpLanguage.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "language.avro")
      vpLocation.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "Location.avro")
      vpAge.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "age.avro")
      vpGender.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "gender.avro")
      vpgivenName.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "givenName.avro")
      vpfriendOf.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "friendOf.avro")
      vpLegalName.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "legalName.avro")
      vpoffers.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "offers.avro")
      vpeligibleRegion.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "eligibleRegion.avro")
      vpincludes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "includes.avro")
      vphomepage.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "homepage.avro")
      vpmakesPurchase.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "makesPurchase.avro")
      vppurchaseFor.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "purchaseFor.avro")
      vppurchaseDate.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "purchaseDate.avro")
      vptotalVotes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "totalVotes.avro")
      vptag.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "tag.avro")
      vptype.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "type.avro")
      vptrailer.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "trailer.avro")
      vpkeywords.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "keywords.avro")
      vphasGenre.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "hasGenre.avro")
      vpdescription.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "description.avro")
      vpurl.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "url.avro")
      vphits.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "hits.avro")
      vpprice.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "price.avro")
      vpvalidThrough.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "validThrough.avro")
      vpPricevaliduntil.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "priceValidUntil.avro")
      vpValidFrom.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "validFrom.avro")
      vpserialNumber.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "serialNumber.avro")
      vpeligibleQuantity.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "eligibleQuantity.avro")
      vppublisher.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "publisher.avro")
      vpartist.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "artist.avro")
      vpfamilyName.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "familyName.avro")
      vpConductor.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/" + "conductor.avro")

      println("Avro VT partitioned and saved! Horizontal partitioning!")


    }


  }
}
