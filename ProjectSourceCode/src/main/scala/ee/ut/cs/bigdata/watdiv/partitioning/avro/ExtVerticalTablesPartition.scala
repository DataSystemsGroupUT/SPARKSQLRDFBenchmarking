package ee.ut.cs.bigdata.watdiv.partitioning.avro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ExtVerticalTablesPartition {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Avro ExtVP Partition")
      .getOrCreate()

    import spark.implicits._

    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase //value = {"Horizontal", "Subject", or "Predicate"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/ExtVP/"

    println("Start Watdiv ExtVP Partitioning Avro...")

    //read tables from HDFS

    //C1
    val SS_caption_hasReview = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/caption/hasReview.orc")
    val SS_contentRating_caption = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/contentRating/caption.orc")
    val SS_text_caption = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/sorg_text/caption.orc")
    val SS_hasReview_caption = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/hasReview/caption.orc")
    val SO_title_hasReview=spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SO/rev_title/hasReview.orc")
    val SS_reviewer_title=spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/reviewer/rev_title.orc")
    val SS_actor_language = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/actor/language.orc")
    val SS_language_actor = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/language/actor.orc")

    //C2 FOR 10M STRUCTURE
    val SO_eligibleRegion_offers = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SO/eligibleRegion/offers.orc")
    val SS_offers_legalName = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/offers/legalName.orc")
    val VP_LegalName = spark.read.format("orc").load(s"$path/VP/VHDFS/ORC/legalName.orc")
    val OS_includes_hasReview = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/OS/includes/hasReview.orc")
    val OS_hasReview_totalVotes = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/OS/hasReview/totalVotes.orc")
    val SO_totalVotes_hasReview = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SO/totalVotes/hasReview.orc")
    val OS_purchaseFor_hasReview = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/OS/purchaseFor/hasReview.orc")
    val SS_makesPurchase_homepage = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/makesPurchase/homepage.orc")
    val SS_jobTitle_homepage = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/jobTitle/homepage.orc")
    val SS_homepage_jobTitle = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/homepage/jobTitle.orc")

    //C3 FOR 10M STRUCTURE
    val SS_Location_likes = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/Location/likes.orc")
    val SS_age_likes = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/age/likes.orc")
    val SS_gender_likes = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/gender/likes.orc")
    val SS_givenName_likes = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/givenName/likes.orc")
    val SS_likes_Location = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/likes/Location.orc")
    val SS_friendOf_likes = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/friendOf/likes.orc")


    //F1 FOR 10M STRUCTURE
    val SO_tag_hasGenre = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SO/tag/hasGenre.orc")
    val SO_type_hasGenre = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SO/type/hasGenre.orc")
    val SS_hasGenre_trailer = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/hasGenre/trailer.orc")
    val SS_type_trailer = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/type/trailer.orc")
    val SS_keywords_trailer = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/keywords/trailer.orc")
    val SS_trailer_keywords = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/trailer/keywords.orc")

    //F2 FOR 10M STRUCTURE
    val SS_hasGenre_caption = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/hasGenre/caption.orc")
    val SS_homepage_caption = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/homepage/caption.orc")
    val SS_caption_homepage = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/caption/homepage.orc")
    val SS_description_caption = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/sorg_description/caption.orc")
    val SS_title_caption = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/og_title/caption.orc")
    val SS_type_caption = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/type/caption.orc")
    val SO_url_homepage = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SO/url/homepage.orc")
    val SO_hits_homepage = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SO/hits/homepage.orc")


    //F3 FOR 10M STRUCTURE
    val SS_hasGenre_contentSize = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/hasGenre/contentSize.orc")
    val SS_contentRating_contentSize = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/contentRating/contentSize.orc")
    val SS_contentSize_contentRating = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/contentSize/contentRating.orc")
    val OS_purchaseFor_contentSize = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/OS/purchaseFor/contentSize.orc")
    val OS_makesPurchase_purchaseDate = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/OS/makesPurchase/purchaseDate.orc")
    val VP_purchaseDate = spark.read.format("orc").load(s"$path/VP/VHDFS/ORC/purchaseDate.orc")


    //F4 FOR 10M STRUCTURE
    val SO_language_homepage = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SO/language/homepage.orc")
    val SS_homepage_contentSize = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/homepage/contentSize.orc")
    val SS_tag_contentSize = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/tag/contentSize.orc")
    val SS_contentSize_homepage = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/contentSize/homepage.orc")
    val SS_description_contentSize = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/sorg_description/contentSize.orc")
    val OS_includes_contentSize = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/OS/includes/contentSize.orc")
    val OS_likes_contentSize = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/OS/likes/contentSize.orc")

    //F5 FOR 10M STRUCTURE
    val OS_offers_validThrough = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/OS/offers/validThrough.orc")
    val SO_validThrough_offers = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SO/validThrough/offers.orc")
    val SS_includes_validThrough = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/includes/validThrough.orc")
    val SO_title_includes = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SO/og_title/includes.orc")
    val SO_type_includes = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SO/type/includes.orc")
    val SS_price_validThrough = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/price/validThrough.orc")


    //L1 FOR 10M STRUCTURE
    val SS_subscribes_likes = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/subscribes/likes.orc")
    val OS_likes_caption = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/OS/likes/caption.orc")
    val SO_caption_likes = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SO/caption/likes.orc")


    //L2 FOR 10M STRUCTURE
    val SS_nationality_likes = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/nationality/likes.orc")
    val SS_likes_nationality = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/likes/nationality.orc")
    val VP_parentCountry = spark.read.format("orc").load(s"$path/VP/VHDFS/ORC/parentCountry.orc")

    //L3 FOR 10M STRUCTURE
    val SS_likes_subscribes = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/likes/subscribes.orc")

    //L4 FOR 10M STRUCTURE
    val SS_tag_caption = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/tag/caption.orc")
    val SS_caption_tag = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/caption/tag.orc")

    //L5 FOR 10M STRUCTURE
    val SS_nationality_jobTitle = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/nationality/jobTitle.orc")
    val SS_jobTitle_nationality = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/jobTitle/nationality.orc")

    //S1
    val OS_offers_priceValidUntil = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/OS/offers/priceValidUntil.orc")
    val SS_validFrom_priceValidUntil = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/validFrom/priceValidUntil.orc")
    val SS_priceValidUntil_validFrom = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/priceValidUntil/validFrom.orc")
    val SS_validThrough_priceValidUntil = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/validThrough/priceValidUntil.orc")
    val SS_includes_priceValidUntil = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/includes/priceValidUntil.orc")
    val SS_price_priceValidUntil = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/price/priceValidUntil.orc")
    val SS_serialNumber_priceValidUntil = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/serialNumber/priceValidUntil.orc")
    val SS_eligibleQuantity_priceValidUntil = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/eligibleQuantity/priceValidUntil.orc")
    val SS_eligibleRegion_priceValidUntil = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/eligibleRegion/priceValidUntil.orc")

    //S2
    val SS_nationality_Location = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/nationality/Location.orc")
    val SS_type_nationality = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/type/Location.orc")
    val SS_Location_nationality = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/Location/nationality.orc")
    val SS_gender_nationality = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/gender/nationality.orc")

    //S3
    val SS_type_publisher = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/type/publisher.orc")
    val SS_caption_publisher = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/caption/publisher.orc")
    val SS_publisher_caption = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/publisher/caption.orc")
    val SS_hasGenre_publisher = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/hasGenre/publisher.orc")

    //S4
    val SO_nationality_artist = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SO/nationality/artist.orc")
    val SO_age_artist = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SO/age/artist.orc")
    val OS_artist_nationality = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/OS/artist/nationality.orc")
    val SO_familyName_artist = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SO/familyName/artist.orc")


    //S5
    val SS_language_keywords = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/language/keywords.orc")
    val SS_type_language = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/type/language.orc")
    val SS_keywords_language = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/keywords/language.orc")
    val SS_description_language = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/sorg_description/language.orc")

    //S6
    val SS_hasGenre_conductor = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/hasGenre/conductor.orc")
    val VP_conductor = spark.read.format("orc").load(s"$path/VP/VHDFS/ORC/conductor.orc")
    val SS_type_conductor = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/type/conductor.orc")


    //S7
    val OS_likes_text = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/OS/likes/sorg_text.orc")
    val SO_text_likes = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SO/sorg_text/likes.orc")
    val SS_type_text = spark.read.format("orc").load(s"$path/ExtVP/VHDFS/ORC/SS/type/sorg_text.orc")

    println("WatDiv VP Tables Read!")

    //partition and save on HDFS
    if (partitionType == "subject") {

          //C1
    SS_caption_hasReview.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_caption_hasReview.avro")
    SS_contentRating_caption.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_contentRating_caption.avro")
    SS_text_caption.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_text_caption.avro")
    SS_hasReview_caption.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_hasReview_caption.avro")
    SO_title_hasReview.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SO_title_hasReview.avro")
    SS_reviewer_title.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_reviewer_title.avro")
    SS_actor_language.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_actor_language.avro")
    SS_language_actor.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_language_actor.avro")


    //C2
    SO_eligibleRegion_offers.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SO_eligibleRegion_offers.avro")
    SS_offers_legalName.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_offers_legalName.avro")
    VP_LegalName.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/VP_LegalName.avro")
    OS_includes_hasReview.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/OS_includes_hasReview.avro")
    OS_hasReview_totalVotes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/OS_hasReview_totalVotes.avro")
    SO_totalVotes_hasReview.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SO_totalVotes_hasReview.avro")
    OS_purchaseFor_hasReview.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/OS_purchaseFor_hasReview.avro")
    SS_makesPurchase_homepage.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_makesPurchase_homepage.avro")
    SS_jobTitle_homepage.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_jobTitle_homepage.avro")
    SS_homepage_jobTitle.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_homepage_jobTitle.avro")


    //C3
    SS_Location_likes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_Location_likes.avro")
    SS_age_likes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_age_likes.avro")
    SS_gender_likes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_gender_likes.avro")
    SS_givenName_likes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_givenName_likes.avro")
    SS_likes_Location.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_likes_Location.avro")
    SS_friendOf_likes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_friendOf_likes.avro")

    //F1
    SO_tag_hasGenre.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SO_tag_hasGenre.avro")
    SO_type_hasGenre.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SO_type_hasGenre.avro")
    SS_hasGenre_trailer.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_hasGenre_trailer.avro")
    SS_type_trailer.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_type_trailer.avro")
    SS_keywords_trailer.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_keywords_trailer.avro")
    SS_trailer_keywords.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_trailer_keywords.avro")

    //F2
    SS_hasGenre_caption.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_hasGenre_caption.avro")
    SS_homepage_caption.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_homepage_caption.avro")
    SS_caption_homepage.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_caption_homepage.avro")
    SS_description_caption.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_description_caption.avro")
    SS_title_caption.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_title_caption.avro")
    SS_type_caption.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_type_caption.avro")
    SO_url_homepage.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SO_url_homepage.avro")
    SO_hits_homepage.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SO_hits_homepage.avro")

    //F3
    SS_hasGenre_contentSize.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_hasGenre_contentSize.avro")
    SS_contentRating_contentSize.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_contentRating_contentSize.avro")
    SS_contentSize_contentRating.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_contentSize_contentRating.avro")
    OS_purchaseFor_contentSize.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/OS_purchaseFor_contentSize.avro")
    OS_makesPurchase_purchaseDate.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/OS_makesPurchase_purchaseDate.avro")
    VP_purchaseDate.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/VP_purchaseDate.avro")

    //F4
    SO_language_homepage.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SO_language_homepage.avro")
    SS_homepage_contentSize.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_homepage_contentSize.avro")
    SS_tag_contentSize.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_tag_contentSize.avro")
    SS_contentSize_homepage.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_contentSize_homepage.avro")
    SS_description_contentSize.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_description_contentSize.avro")
    OS_includes_contentSize.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/OS_includes_contentSize.avro")
    OS_likes_contentSize.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/OS_likes_contentSize.avro")

    //F5
    OS_offers_validThrough.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/OS_offers_validThrough.avro")
    SO_validThrough_offers.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SO_validThrough_offers.avro")
    SS_includes_validThrough.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_includes_validThrough.avro")
    SO_title_includes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SO_title_includes.avro")
    SO_type_includes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SO_type_includes.avro")
    SS_price_validThrough.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_price_validThrough.avro")

    //L1
    SS_subscribes_likes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_subscribes_likes.avro")
    OS_likes_caption.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/OS_likes_caption.avro")
    SO_caption_likes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SO_caption_likes.avro")

    //L2
    SS_nationality_likes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_nationality_likes.avro")
    SS_likes_nationality.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_likes_nationality.avro")
    VP_parentCountry.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/VP_parentCountry.avro")

    //L3
     SS_likes_subscribes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_likes_subscribes.avro")

    //L4
    SS_tag_caption.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_tag_caption.avro")
    SS_caption_tag.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_caption_tag.avro")

    //L5
    SS_nationality_jobTitle.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_nationality_jobTitle.avro")
    SS_jobTitle_nationality.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_jobTitle_nationality.avro")


    //S1
    OS_offers_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/OS_offers_priceValidUntil.avro")
    SS_validFrom_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_validFrom_priceValidUntil.avro")
    SS_priceValidUntil_validFrom.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_priceValidUntil_validFrom.avro")
    SS_validThrough_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_validThrough_priceValidUntil.avro")
    SS_includes_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_includes_priceValidUntil.avro")
    SS_price_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_price_priceValidUntil.avro")
    SS_serialNumber_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_serialNumber_priceValidUntil.avro")
    SS_eligibleQuantity_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_eligibleQuantity_priceValidUntil.avro")
    SS_eligibleRegion_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_eligibleRegion_priceValidUntil.avro")


    //S2
    SS_nationality_Location.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_nationality_Location.avro")
    SS_type_nationality.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_type_nationality.avro")
    SS_Location_nationality.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_Location_nationality.avro")
    SS_gender_nationality.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_gender_nationality.avro")


    //S3
    SS_type_publisher.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_type_publisher.avro")
    SS_caption_publisher.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_caption_publisher.avro")
    SS_publisher_caption.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_publisher_caption.avro")
    SS_hasGenre_publisher.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_hasGenre_publisher.avro")



    //S4
    SO_nationality_artist.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SO_nationality_artist.avro")
    SO_age_artist.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SO_age_artist.avro")
    OS_artist_nationality.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/OS_artist_nationality.avro")
    SO_familyName_artist.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SO_familyName_artist.avro")


    //S5
    SS_language_keywords.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_language_keywords.avro")
    SS_type_language.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_type_language.avro")
    SS_keywords_language.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_keywords_language.avro")
    SS_description_language.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_description_language.avro")

    //S6
    SS_hasGenre_conductor.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_hasGenre_conductor.avro")
    VP_conductor.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/VP_conductor.avro")
    SS_type_conductor.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_type_conductor.avro")


    //S7
    OS_likes_text.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/OS_likes_text.avro")
    SO_text_likes.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SO_text_likes.avro")
    SS_type_text.repartition(84, $"Subject").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Subject/Avro/SS_type_text.avro")

      println("Avro ExtVP partitioned and saved! Subject based Partitioning")
    }

    else if (partitionType == "horizontal") {

    //C1
    SS_caption_hasReview.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_caption_hasReview.avro")
    SS_contentRating_caption.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_contentRating_caption.avro")
    SS_text_caption.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_text_caption.avro")
    SS_hasReview_caption.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_hasReview_caption.avro")
    SO_title_hasReview.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SO_title_hasReview.avro")
    SS_reviewer_title.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_reviewer_title.avro")
    SS_actor_language.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_actor_language.avro")
    SS_language_actor.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_language_actor.avro")


    //C2
    SO_eligibleRegion_offers.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SO_eligibleRegion_offers.avro")
    SS_offers_legalName.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_offers_legalName.avro")
    VP_LegalName.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/VP_LegalName.avro")
    OS_includes_hasReview.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/OS_includes_hasReview.avro")
    OS_hasReview_totalVotes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/OS_hasReview_totalVotes.avro")
    SO_totalVotes_hasReview.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SO_totalVotes_hasReview.avro")
    OS_purchaseFor_hasReview.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/OS_purchaseFor_hasReview.avro")
    SS_makesPurchase_homepage.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_makesPurchase_homepage.avro")
    SS_jobTitle_homepage.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_jobTitle_homepage.avro")
    SS_homepage_jobTitle.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_homepage_jobTitle.avro")


    //C3
    SS_Location_likes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_Location_likes.avro")
    SS_age_likes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_age_likes.avro")
    SS_gender_likes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_gender_likes.avro")
    SS_givenName_likes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_givenName_likes.avro")
    SS_likes_Location.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_likes_Location.avro")
    SS_friendOf_likes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_friendOf_likes.avro")

    //F1
    SO_tag_hasGenre.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SO_tag_hasGenre.avro")
    SO_type_hasGenre.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SO_type_hasGenre.avro")
    SS_hasGenre_trailer.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_hasGenre_trailer.avro")
    SS_type_trailer.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_type_trailer.avro")
    SS_keywords_trailer.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_keywords_trailer.avro")
    SS_trailer_keywords.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_trailer_keywords.avro")

    //F2
    SS_hasGenre_caption.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_hasGenre_caption.avro")
    SS_homepage_caption.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_homepage_caption.avro")
    SS_caption_homepage.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_caption_homepage.avro")
    SS_description_caption.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_description_caption.avro")
    SS_title_caption.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_title_caption.avro")
    SS_type_caption.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_type_caption.avro")
    SO_url_homepage.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SO_url_homepage.avro")
    SO_hits_homepage.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SO_hits_homepage.avro")

    //F3
    SS_hasGenre_contentSize.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_hasGenre_contentSize.avro")
    SS_contentRating_contentSize.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_contentRating_contentSize.avro")
    SS_contentSize_contentRating.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_contentSize_contentRating.avro")
    OS_purchaseFor_contentSize.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/OS_purchaseFor_contentSize.avro")
    OS_makesPurchase_purchaseDate.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/OS_makesPurchase_purchaseDate.avro")
    VP_purchaseDate.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/VP_purchaseDate.avro")

    //F4
    SO_language_homepage.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SO_language_homepage.avro")
    SS_homepage_contentSize.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_homepage_contentSize.avro")
    SS_tag_contentSize.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_tag_contentSize.avro")
    SS_contentSize_homepage.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_contentSize_homepage.avro")
    SS_description_contentSize.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_description_contentSize.avro")
    OS_includes_contentSize.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/OS_includes_contentSize.avro")
    OS_likes_contentSize.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/OS_likes_contentSize.avro")

    //F5
    OS_offers_validThrough.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/OS_offers_validThrough.avro")
    SO_validThrough_offers.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SO_validThrough_offers.avro")
    SS_includes_validThrough.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_includes_validThrough.avro")
    SO_title_includes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SO_title_includes.avro")
    SO_type_includes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SO_type_includes.avro")
    SS_price_validThrough.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_price_validThrough.avro")

    //L1
    SS_subscribes_likes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_subscribes_likes.avro")
    OS_likes_caption.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/OS_likes_caption.avro")
    SO_caption_likes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SO_caption_likes.avro")

    //L2
    SS_nationality_likes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_nationality_likes.avro")
    SS_likes_nationality.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_likes_nationality.avro")
    VP_parentCountry.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/VP_parentCountry.avro")

    //L3
     SS_likes_subscribes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_likes_subscribes.avro")

    //L4
    SS_tag_caption.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_tag_caption.avro")
    SS_caption_tag.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_caption_tag.avro")

    //L5
    SS_nationality_jobTitle.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_nationality_jobTitle.avro")
    SS_jobTitle_nationality.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_jobTitle_nationality.avro")


    //S1
    OS_offers_priceValidUntil.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/OS_offers_priceValidUntil.avro")
    SS_validFrom_priceValidUntil.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_validFrom_priceValidUntil.avro")
    SS_priceValidUntil_validFrom.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_priceValidUntil_validFrom.avro")
    SS_validThrough_priceValidUntil.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_validThrough_priceValidUntil.avro")
    SS_includes_priceValidUntil.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_includes_priceValidUntil.avro")
    SS_price_priceValidUntil.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_price_priceValidUntil.avro")
    SS_serialNumber_priceValidUntil.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_serialNumber_priceValidUntil.avro")
    SS_eligibleQuantity_priceValidUntil.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_eligibleQuantity_priceValidUntil.avro")
    SS_eligibleRegion_priceValidUntil.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_eligibleRegion_priceValidUntil.avro")


    //S2
    SS_nationality_Location.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_nationality_Location.avro")
    SS_type_nationality.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_type_nationality.avro")
    SS_Location_nationality.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_Location_nationality.avro")
    SS_gender_nationality.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_gender_nationality.avro")


    //S3
    SS_type_publisher.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_type_publisher.avro")
    SS_caption_publisher.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_caption_publisher.avro")
    SS_publisher_caption.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_publisher_caption.avro")
    SS_hasGenre_publisher.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_hasGenre_publisher.avro")



    //S4
    SO_nationality_artist.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SO_nationality_artist.avro")
    SO_age_artist.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SO_age_artist.avro")
    OS_artist_nationality.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/OS_artist_nationality.avro")
    SO_familyName_artist.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SO_familyName_artist.avro")


    //S5
    SS_language_keywords.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_language_keywords.avro")
    SS_type_language.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_type_language.avro")
    SS_keywords_language.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_keywords_language.avro")
    SS_description_language.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_description_language.avro")

    //S6
    SS_hasGenre_conductor.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_hasGenre_conductor.avro")
    VP_conductor.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/VP_conductor.avro")
    SS_type_conductor.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_type_conductor.avro")


    //S7
    OS_likes_text.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/OS_likes_text.avro")
    SO_text_likes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SO_text_likes.avro")
    SS_type_text.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path+ "Horizontal/Avro/SS_type_text.avro")

    println("Avro ExtVP partitioned and saved! Horizontal partitioning")

    }

  }
}
