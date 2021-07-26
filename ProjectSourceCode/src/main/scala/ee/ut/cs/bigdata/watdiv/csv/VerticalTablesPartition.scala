package ee.ut.cs.bigdata.watdiv.csv

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
      .appName("RDFBench Predicate/CSV VT Partitioned")
      .getOrCreate()

    import spark.implicits._

    val ds = args(0) // data size
    val partitionType = args(1).toLowerCase // horizontal, predicate or subject

    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/VP/"

    //read tables from HDFS

    val vpTabl_subscribes = spark.read.option("header", true).csv(path + "VHDFS/CSV/subscribes.csv").toDF()
    val vpTabl_likes = spark.read.option("header", true).csv(path + "VHDFS/CSV/likes.csv").toDF()
    val vpSubscribes = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "subscribes.csv").toDF()
    val vpLikes = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "likes.csv").toDF()
    val vpCaption = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "caption.csv").toDF()
    val vpParentCount = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "parentCountry.csv").toDF()
    val vpNationality = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "nationality.csv").toDF()
    val vpjobTitle = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "jobTitle.csv").toDF()
    val vpText = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "text.csv").toDF()
    val vpcontentRating = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "contentRating.csv").toDF()
    val vpcontentSize = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "contentSize.csv").toDF()
    val vphasReview = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "hasReview.csv").toDF()
    val vpTitle = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "og_title.csv").toDF()
    val vpRevTitle = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "rev_title.csv").toDF()
    val vpreviewer = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "reviewer.csv").toDF()
    val vpActor = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "actor.csv").toDF()
    val vpLanguage = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "language.csv").toDF()
    val vpLocation = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "Location.csv").toDF()
    val vpAge = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "age.csv").toDF()
    val vpGender = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "gender.csv").toDF()
    val vpgivenName = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "givenName.csv").toDF()
    val vpfriendOf = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "friendOf.csv").toDF()
    val vpLegalName = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "legalName.csv").toDF()
    val vpoffers = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "offers.csv").toDF()
    val vpeligibleRegion = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "eligibleRegion.csv").toDF()
    val vpincludes = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "includes.csv").toDF()
    val vphomepage = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "homepage.csv").toDF()
    val vpmakesPurchase = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "makesPurchase.csv").toDF()
    val vppurchaseFor = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "purchaseFor.csv").toDF()
    val vppurchaseDate = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "purchaseDate.csv").toDF()
    val vptotalVotes = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "totalVotes.csv").toDF()
    val vptag = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "tag.csv").toDF()
    val vptype = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "type.csv").toDF()
    val vptrailer = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "trailer.csv").toDF()
    val vpkeywords = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "keywords.csv").toDF()
    val vphasGenre = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "hasGenre.csv").toDF()
    val vpdescription = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "description.csv").toDF()
    val vpurl = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "url.csv").toDF()
    val vphits = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "hits.csv").toDF()
    val vpprice = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "price.csv").toDF()
    val vpvalidThrough = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "validThrough.csv").toDF()
    val vpPricevaliduntil = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "priceValidUntil.csv").toDF()
    val vpValidFrom = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "validFrom.csv").toDF()
    val vpserialNumber = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "serialNumber.csv").toDF()
    val vpeligibleQuantity = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "eligibleQuantity.csv").toDF()
    val vppublisher = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "publisher.csv").toDF()
    val vpartist = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "artist.csv").toDF()
    val vpfamilyName = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "familyName.csv").toDF()
    val vpConductor = spark.read.option("header", true).csv(path + "VHDFS/CSV/" + "conductor.csv").toDF()


    import spark.implicits._

    //partition and save on HDFS
    if (partitionType == "subject") {
      vpTabl_subscribes.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/subscribes.csv")
      vpTabl_likes.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/likes.csv")
      vpSubscribes.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "subscribes.csv")
      vpLikes.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "likes.csv")
      vpCaption.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "caption.csv")
      vpParentCount.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "parentCountry.csv")
      vpNationality.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "nationality.csv")
      vpjobTitle.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "jobTitle.csv")
      vpText.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "text.csv")
      vpcontentRating.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "contentRating.csv")
      vpcontentSize.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "contentSize.csv")
      vphasReview.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "hasReview.csv")
      vpTitle.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "og_title.csv")
      vpRevTitle.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "rev_title.csv")
      vpreviewer.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "reviewer.csv")
      vpActor.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "actor.csv")
      vpLanguage.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "language.csv")
      vpLocation.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "Location.csv")
      vpAge.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "age.csv")
      vpGender.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "gender.csv")
      vpgivenName.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "givenName.csv")
      vpfriendOf.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "friendOf.csv")
      vpLegalName.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "legalName.csv")
      vpoffers.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "offers.csv")
      vpeligibleRegion.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "eligibleRegion.csv")
      vpincludes.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "includes.csv")
      vphomepage.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "homepage.csv")
      vpmakesPurchase.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "makesPurchase.csv")
      vppurchaseFor.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "purchaseFor.csv")
      vppurchaseDate.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "purchaseDate.csv")
      vptotalVotes.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "totalVotes.csv")
      vptag.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "tag.csv")
      vptype.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "type.csv")
      vptrailer.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "trailer.csv")
      vpkeywords.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "keywords.csv")
      vphasGenre.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "hasGenre.csv")
      vpdescription.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "description.csv")
      vpurl.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "url.csv")
      vphits.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "hits.csv")
      vpprice.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "price.csv")
      vpvalidThrough.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "validThrough.csv")
      vpPricevaliduntil.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "priceValidUntil.csv")
      vpValidFrom.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "validFrom.csv")
      vpserialNumber.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "serialNumber.csv")
      vpeligibleQuantity.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "eligibleQuantity.csv")
      vppublisher.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "publisher.csv")
      vpartist.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "artist.csv")
      vpfamilyName.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "familyName.csv")
      vpConductor.repartition(84, $"Subject").write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/" + "conductor.csv")
    }


    else if (partitionType == "horizontal") {

      vpTabl_subscribes.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/subscribes.csv")
      vpTabl_likes.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/likes.csv")
      vpSubscribes.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "subscribes.csv")
      vpLikes.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "likes.csv")
      vpCaption.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "caption.csv")
      vpParentCount.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "parentCountry.csv")
      vpNationality.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "nationality.csv")
      vpjobTitle.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "jobTitle.csv")
      vpText.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "text.csv")
      vpcontentRating.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "contentRating.csv")
      vpcontentSize.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "contentSize.csv")
      vphasReview.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "hasReview.csv")
      vpTitle.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "og_title.csv")
      vpRevTitle.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "rev_title.csv")
      vpreviewer.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "reviewer.csv")
      vpActor.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "actor.csv")
      vpLanguage.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "language.csv")
      vpLocation.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "Location.csv")
      vpAge.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "age.csv")
      vpGender.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "gender.csv")
      vpgivenName.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "givenName.csv")
      vpfriendOf.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "friendOf.csv")
      vpLegalName.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "legalName.csv")
      vpoffers.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "offers.csv")
      vpeligibleRegion.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "eligibleRegion.csv")
      vpincludes.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "includes.csv")
      vphomepage.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "homepage.csv")
      vpmakesPurchase.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "makesPurchase.csv")
      vppurchaseFor.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "purchaseFor.csv")
      vppurchaseDate.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "purchaseDate.csv")
      vptotalVotes.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "totalVotes.csv")
      vptag.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "tag.csv")
      vptype.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "type.csv")
      vptrailer.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "trailer.csv")
      vpkeywords.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "keywords.csv")
      vphasGenre.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "hasGenre.csv")
      vpdescription.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "description.csv")
      vpurl.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "url.csv")
      vphits.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "hits.csv")
      vpprice.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "price.csv")
      vpvalidThrough.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "validThrough.csv")
      vpPricevaliduntil.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "priceValidUntil.csv")
      vpValidFrom.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "validFrom.csv")
      vpserialNumber.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "serialNumber.csv")
      vpeligibleQuantity.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "eligibleQuantity.csv")
      vppublisher.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "publisher.csv")
      vpartist.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "artist.csv")
      vpfamilyName.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "familyName.csv")
      vpConductor.repartition(84).write.option("header", true).format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/" + "conductor.csv")

    }


  }
}
