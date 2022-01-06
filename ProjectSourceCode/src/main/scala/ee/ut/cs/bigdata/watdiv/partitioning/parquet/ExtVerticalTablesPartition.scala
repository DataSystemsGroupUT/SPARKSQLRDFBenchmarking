package ee.ut.cs.bigdata.watdiv.partitioning.parquet

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ExtVerticalTablesPartition {
  def main(args: Array[String]): Unit = {

    println("Start Watdiv ExtVP Partitioning Parquet...")

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Parquet ExtVP Partition")
      .getOrCreate()

    import spark.implicits._

    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase //value = {"Horizontal", "Subject", or "Predicate"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/"

    //read tables from HDFS

    //C1 FOR 10M STRUCTURE (WE NEED TO FIX THE PATH)
    val SS_caption_hasReview = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/caption/hasReview.parquet")
    val SS_contentRating_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/contentRating/caption.parquet")
    val SS_text_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/sorg_text/caption.parquet")
    val SS_hasReview_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/hasReview/caption.parquet")
    val SO_title_hasReview=spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/rev_title/hasReview.parquet")
    val SS_reviewer_title=spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/reviewer/rev_title.parquet")
    val SS_actor_language = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/actor/language.parquet")
    val SS_language_actor = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/language/actor.parquet")

    //C2 FOR 10M STRUCTURE
    val SO_eligibleRegion_offers = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/eligibleRegion/offers.parquet")
    val SS_offers_legalName = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/offers/legalName.parquet")
    val VP_LegalName = spark.read.format("parquet").load(s"$path/VP/VHDFS/Parquet/legalName.parquet")
    val OS_includes_hasReview = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/includes/hasReview.parquet")
    val OS_hasReview_totalVotes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/hasReview/totalVotes.parquet")
    val SO_totalVotes_hasReview = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/totalVotes/hasReview.parquet")
    val OS_purchaseFor_hasReview = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/purchaseFor/hasReview.parquet")
    val SS_makesPurchase_homepage = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/makesPurchase/homepage.parquet")
    val SS_jobTitle_homepage = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/jobTitle/homepage.parquet")
    val SS_homepage_jobTitle = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/homepage/jobTitle.parquet")

    //C3 FOR 10M STRUCTURE
    val SS_Location_likes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/Location/likes.parquet")
    val SS_age_likes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/age/likes.parquet")
    val SS_gender_likes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/gender/likes.parquet")
    val SS_givenName_likes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/givenName/likes.parquet")
    val SS_likes_Location = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/likes/Location.parquet")
    val SS_friendOf_likes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/friendOf/likes.parquet")


    //F1 FOR 10M STRUCTURE
    val SO_tag_hasGenre = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/tag/hasGenre.parquet")
    val SO_type_hasGenre = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/type/hasGenre.parquet")
    val SS_hasGenre_trailer = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/hasGenre/trailer.parquet")
    val SS_type_trailer = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/type/trailer.parquet")
    val SS_keywords_trailer = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/keywords/trailer.parquet")
    val SS_trailer_keywords = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/trailer/keywords.parquet")

    //F2 FOR 10M STRUCTURE
    val SS_hasGenre_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/hasGenre/caption.parquet")
    val SS_homepage_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/homepage/caption.parquet")
    val SS_caption_homepage = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/caption/homepage.parquet")
    val SS_description_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/sorg_description/caption.parquet")
    val SS_title_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/og_title/caption.parquet")
    val SS_type_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/type/caption.parquet")
    val SO_url_homepage = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/url/homepage.parquet")
    val SO_hits_homepage = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/hits/homepage.parquet")


    //F3 FOR 10M STRUCTURE
    val SS_hasGenre_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/hasGenre/contentSize.parquet")
    val SS_contentRating_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/contentRating/contentSize.parquet")
    val SS_contentSize_contentRating = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/contentSize/contentRating.parquet")
    val OS_purchaseFor_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/purchaseFor/contentSize.parquet")
    val OS_makesPurchase_purchaseDate = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/makesPurchase/purchaseDate.parquet")
    val VP_purchaseDate = spark.read.format("parquet").load(s"$path/VP/VHDFS/Parquet/purchaseDate.parquet")


    //F4 FOR 10M STRUCTURE
    val SO_language_homepage = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/language/homepage.parquet")
    val SS_homepage_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/homepage/contentSize.parquet")
    val SS_tag_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/tag/contentSize.parquet")
    val SS_contentSize_homepage = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/contentSize/homepage.parquet")
    val SS_description_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/sorg_description/contentSize.parquet")
    val OS_includes_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/includes/contentSize.parquet")
    val OS_likes_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/likes/contentSize.parquet")

    //F5 FOR 10M STRUCTURE
    val OS_offers_validThrough = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/offers/validThrough.parquet")
    val SO_validThrough_offers = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/validThrough/offers.parquet")
    val SS_includes_validThrough = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/includes/validThrough.parquet")
    val SO_title_includes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/og_title/includes.parquet")
    val SO_type_includes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/type/includes.parquet")
    val SS_price_validThrough = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/price/validThrough.parquet")


    //L1 FOR 10M STRUCTURE
    val SS_subscribes_likes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/subscribes/likes.parquet")
    val OS_likes_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/likes/caption.parquet")
    val SO_caption_likes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/caption/likes.parquet")


    //L2 FOR 10M STRUCTURE
    val SS_nationality_likes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/nationality/likes.parquet")
    val SS_likes_nationality = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/likes/nationality.parquet")
    val VP_parentCountry = spark.read.format("parquet").load(s"$path/VP/VHDFS/Parquet/parentCountry.parquet")

    //L3 FOR 10M STRUCTURE
    val SS_likes_subscribes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/likes/subscribes.parquet")

    //L4 FOR 10M STRUCTURE
    val SS_tag_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/tag/caption.parquet")
    val SS_caption_tag = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/caption/tag.parquet")

    //L5 FOR 10M STRUCTURE
    val SS_nationality_jobTitle = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/nationality/jobTitle.parquet")
    val SS_jobTitle_nationality = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/jobTitle/nationality.parquet")

    //S1
    val OS_offers_priceValidUntil = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/offers/priceValidUntil.parquet")
    val SS_validFrom_priceValidUntil = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/validFrom/priceValidUntil.parquet")
    val SS_priceValidUntil_validFrom = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/priceValidUntil/validFrom.parquet")
    val SS_validThrough_priceValidUntil = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/validThrough/priceValidUntil.parquet")
    val SS_includes_priceValidUntil = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/includes/priceValidUntil.parquet")
    val SS_price_priceValidUntil = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/price/priceValidUntil.parquet")
    val SS_serialNumber_priceValidUntil = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/serialNumber/priceValidUntil.parquet")
    val SS_eligibleQuantity_priceValidUntil = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/eligibleQuantity/priceValidUntil.parquet")
    val SS_eligibleRegion_priceValidUntil = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/eligibleRegion/priceValidUntil.parquet")

    //S2
    val SS_nationality_Location = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/nationality/Location.parquet")
    val SS_type_nationality = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/type/Location.parquet")
    val SS_Location_nationality = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/Location/nationality.parquet")
    val SS_gender_nationality = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/gender/nationality.parquet")

    //S3
    val SS_type_publisher = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/type/publisher.parquet")
    val SS_caption_publisher = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/caption/publisher.parquet")
    val SS_publisher_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/publisher/caption.parquet")
    val SS_hasGenre_publisher = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/hasGenre/publisher.parquet")

    //S4
    val SO_nationality_artist = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/nationality/artist.parquet")
    val SO_age_artist = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/age/artist.parquet")
    val OS_artist_nationality = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/artist/nationality.parquet")
    val SO_familyName_artist = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/familyName/artist.parquet")


    //S5
    val SS_language_keywords = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/language/keywords.parquet")
    val SS_type_language = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/type/language.parquet")
    val SS_keywords_language = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/keywords/language.parquet")
    val SS_description_language = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/sorg_description/language.parquet")

    //S6
    val SS_hasGenre_conductor = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/hasGenre/conductor.parquet")
    val VP_conductor = spark.read.format("parquet").load(s"$path/VP/VHDFS/Parquet/conductor.parquet")
    val SS_type_conductor = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/type/conductor.parquet")


    //S7
    val OS_likes_text = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/likes/sorg_text.parquet")
    val SO_text_likes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/sorg_text/likes.parquet")
    val SS_type_text = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/type/sorg_text.parquet")


    println("WatDiv VP Tables Read!")

    //partition and save on HDFS
    if (partitionType == "subject") {

          //C1
    SS_caption_hasReview.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_caption_hasReview.parquet")
    SS_contentRating_caption.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_contentRating_caption.parquet")
    SS_text_caption.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_text_caption.parquet")
    SS_hasReview_caption.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_hasReview_caption.parquet")
    SO_title_hasReview.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SO_title_hasReview.parquet")
    SS_reviewer_title.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_reviewer_title.parquet")
    SS_actor_language.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_actor_language.parquet")
    SS_language_actor.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_language_actor.parquet")


    //C2
    SO_eligibleRegion_offers.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SO_eligibleRegion_offers.parquet")
    SS_offers_legalName.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_offers_legalName.parquet")
    VP_LegalName.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/VP_LegalName.parquet")
    OS_includes_hasReview.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/OS_includes_hasReview.parquet")
    OS_hasReview_totalVotes.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/OS_hasReview_totalVotes.parquet")
    SO_totalVotes_hasReview.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SO_totalVotes_hasReview.parquet")
    OS_purchaseFor_hasReview.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/OS_purchaseFor_hasReview.parquet")
    SS_makesPurchase_homepage.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_makesPurchase_homepage.parquet")
    SS_jobTitle_homepage.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_jobTitle_homepage.parquet")
    SS_homepage_jobTitle.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_homepage_jobTitle.parquet")


    //C3
    SS_Location_likes.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_Location_likes.parquet")
    SS_age_likes.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_age_likes.parquet")
    SS_gender_likes.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_gender_likes.parquet")
    SS_givenName_likes.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_givenName_likes.parquet")
    SS_likes_Location.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_likes_Location.parquet")
    SS_friendOf_likes.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_friendOf_likes.parquet")

    //F1
    SO_tag_hasGenre.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SO_tag_hasGenre.parquet")
    SO_type_hasGenre.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SO_type_hasGenre.parquet")
    SS_hasGenre_trailer.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_hasGenre_trailer.parquet")
    SS_type_trailer.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_type_trailer.parquet")
    SS_keywords_trailer.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_keywords_trailer.parquet")
    SS_trailer_keywords.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_trailer_keywords.parquet")

    //F2
    SS_hasGenre_caption.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_hasGenre_caption.parquet")
    SS_homepage_caption.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_homepage_caption.parquet")
    SS_caption_homepage.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_caption_homepage.parquet")
    SS_description_caption.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_description_caption.parquet")
    SS_title_caption.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_title_caption.parquet")
    SS_type_caption.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_type_caption.parquet")
    SO_url_homepage.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SO_url_homepage.parquet")
    SO_hits_homepage.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SO_hits_homepage.parquet")

    //F3
    SS_hasGenre_contentSize.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_hasGenre_contentSize.parquet")
    SS_contentRating_contentSize.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_contentRating_contentSize.parquet")
    SS_contentSize_contentRating.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_contentSize_contentRating.parquet")
    OS_purchaseFor_contentSize.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/OS_purchaseFor_contentSize.parquet")
    OS_makesPurchase_purchaseDate.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/OS_makesPurchase_purchaseDate.parquet")
    VP_purchaseDate.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/VP_purchaseDate.parquet")

    //F4
    SO_language_homepage.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SO_language_homepage.parquet")
    SS_homepage_contentSize.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_homepage_contentSize.parquet")
    SS_tag_contentSize.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_tag_contentSize.parquet")
    SS_contentSize_homepage.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_contentSize_homepage.parquet")
    SS_description_contentSize.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_description_contentSize.parquet")
    OS_includes_contentSize.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/OS_includes_contentSize.parquet")
    OS_likes_contentSize.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/OS_likes_contentSize.parquet")

    //F5
    OS_offers_validThrough.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/OS_offers_validThrough.parquet")
    SO_validThrough_offers.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SO_validThrough_offers.parquet")
    SS_includes_validThrough.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_includes_validThrough.parquet")
    SO_title_includes.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SO_title_includes.parquet")
    SO_type_includes.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SO_type_includes.parquet")
    SS_price_validThrough.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_price_validThrough.parquet")

    //L1
    SS_subscribes_likes.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_subscribes_likes.parquet")
    OS_likes_caption.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/OS_likes_caption.parquet")
    SO_caption_likes.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SO_caption_likes.parquet")

    //L2
    SS_nationality_likes.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_nationality_likes.parquet")
    SS_likes_nationality.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_likes_nationality.parquet")
    VP_parentCountry.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/VP_parentCountry.parquet")

    //L3
     SS_likes_subscribes.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_likes_subscribes.parquet")

    //L4
    SS_tag_caption.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_tag_caption.parquet")
    SS_caption_tag.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_caption_tag.parquet")

    //L5
    SS_nationality_jobTitle.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_nationality_jobTitle.parquet")
    SS_jobTitle_nationality.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_jobTitle_nationality.parquet")


    //S1
    OS_offers_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/OS_offers_priceValidUntil.parquet")
    SS_validFrom_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_validFrom_priceValidUntil.parquet")
    SS_priceValidUntil_validFrom.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_priceValidUntil_validFrom.parquet")
    SS_validThrough_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_validThrough_priceValidUntil.parquet")
    SS_includes_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_includes_priceValidUntil.parquet")
    SS_price_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_price_priceValidUntil.parquet")
    SS_serialNumber_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_serialNumber_priceValidUntil.parquet")
    SS_eligibleQuantity_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_eligibleQuantity_priceValidUntil.parquet")
    SS_eligibleRegion_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_eligibleRegion_priceValidUntil.parquet")


    //S2
    SS_nationality_Location.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_nationality_Location.parquet")
    SS_type_nationality.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_type_nationality.parquet")
    SS_Location_nationality.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_Location_nationality.parquet")
    SS_gender_nationality.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_gender_nationality.parquet")


    //S3
    SS_type_publisher.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_type_publisher.parquet")
    SS_caption_publisher.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_caption_publisher.parquet")
    SS_publisher_caption.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_publisher_caption.parquet")
    SS_hasGenre_publisher.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_hasGenre_publisher.parquet")



    //S4
    SO_nationality_artist.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SO_nationality_artist.parquet")
    SO_age_artist.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SO_age_artist.parquet")
    OS_artist_nationality.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/OS_artist_nationality.parquet")
    SO_familyName_artist.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SO_familyName_artist.parquet")


    //S5
    SS_language_keywords.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_language_keywords.parquet")
    SS_type_language.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_type_language.parquet")
    SS_keywords_language.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_keywords_language.parquet")
    SS_description_language.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_description_language.parquet")

    //S6
    SS_hasGenre_conductor.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_hasGenre_conductor.parquet")
    VP_conductor.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/VP_conductor.parquet")
    SS_type_conductor.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_type_conductor.parquet")


    //S7
    OS_likes_text.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/OS_likes_text.parquet")
    SO_text_likes.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SO_text_likes.parquet")
    SS_type_text.repartition(84, $"Subject").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/Parquet/SS_type_text.parquet")

      println("Parquet ExtVP partitioned and saved! Subject based Partitioning")
    }

    else if (partitionType == "horizontal") {

    //C1
    SS_caption_hasReview.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_caption_hasReview.parquet")
    SS_contentRating_caption.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_contentRating_caption.parquet")
    SS_text_caption.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_text_caption.parquet")
    SS_hasReview_caption.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_hasReview_caption.parquet")
    SO_title_hasReview.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SO_title_hasReview.parquet")
    SS_reviewer_title.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_reviewer_title.parquet")
    SS_actor_language.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_actor_language.parquet")
    SS_language_actor.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_language_actor.parquet")


    //C2
    SO_eligibleRegion_offers.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SO_eligibleRegion_offers.parquet")
    SS_offers_legalName.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_offers_legalName.parquet")
    VP_LegalName.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/VP_LegalName.parquet")
    OS_includes_hasReview.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/OS_includes_hasReview.parquet")
    OS_hasReview_totalVotes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/OS_hasReview_totalVotes.parquet")
    SO_totalVotes_hasReview.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SO_totalVotes_hasReview.parquet")
    OS_purchaseFor_hasReview.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/OS_purchaseFor_hasReview.parquet")
    SS_makesPurchase_homepage.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_makesPurchase_homepage.parquet")
    SS_jobTitle_homepage.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_jobTitle_homepage.parquet")
    SS_homepage_jobTitle.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_homepage_jobTitle.parquet")


    //C3
    SS_Location_likes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_Location_likes.parquet")
    SS_age_likes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_age_likes.parquet")
    SS_gender_likes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_gender_likes.parquet")
    SS_givenName_likes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_givenName_likes.parquet")
    SS_likes_Location.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_likes_Location.parquet")
    SS_friendOf_likes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_friendOf_likes.parquet")

    //F1
    SO_tag_hasGenre.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SO_tag_hasGenre.parquet")
    SO_type_hasGenre.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SO_type_hasGenre.parquet")
    SS_hasGenre_trailer.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_hasGenre_trailer.parquet")
    SS_type_trailer.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_type_trailer.parquet")
    SS_keywords_trailer.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_keywords_trailer.parquet")
    SS_trailer_keywords.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_trailer_keywords.parquet")

    //F2
    SS_hasGenre_caption.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_hasGenre_caption.parquet")
    SS_homepage_caption.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_homepage_caption.parquet")
    SS_caption_homepage.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_caption_homepage.parquet")
    SS_description_caption.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_description_caption.parquet")
    SS_title_caption.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_title_caption.parquet")
    SS_type_caption.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_type_caption.parquet")
    SO_url_homepage.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SO_url_homepage.parquet")
    SO_hits_homepage.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SO_hits_homepage.parquet")

    //F3
    SS_hasGenre_contentSize.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_hasGenre_contentSize.parquet")
    SS_contentRating_contentSize.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_contentRating_contentSize.parquet")
    SS_contentSize_contentRating.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_contentSize_contentRating.parquet")
    OS_purchaseFor_contentSize.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/OS_purchaseFor_contentSize.parquet")
    OS_makesPurchase_purchaseDate.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/OS_makesPurchase_purchaseDate.parquet")
    VP_purchaseDate.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/VP_purchaseDate.parquet")

    //F4
    SO_language_homepage.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SO_language_homepage.parquet")
    SS_homepage_contentSize.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_homepage_contentSize.parquet")
    SS_tag_contentSize.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_tag_contentSize.parquet")
    SS_contentSize_homepage.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_contentSize_homepage.parquet")
    SS_description_contentSize.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_description_contentSize.parquet")
    OS_includes_contentSize.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/OS_includes_contentSize.parquet")
    OS_likes_contentSize.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/OS_likes_contentSize.parquet")

    //F5
    OS_offers_validThrough.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/OS_offers_validThrough.parquet")
    SO_validThrough_offers.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SO_validThrough_offers.parquet")
    SS_includes_validThrough.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_includes_validThrough.parquet")
    SO_title_includes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SO_title_includes.parquet")
    SO_type_includes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SO_type_includes.parquet")
    SS_price_validThrough.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_price_validThrough.parquet")

    //L1
    SS_subscribes_likes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_subscribes_likes.parquet")
    OS_likes_caption.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/OS_likes_caption.parquet")
    SO_caption_likes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SO_caption_likes.parquet")

    //L2
    SS_nationality_likes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_nationality_likes.parquet")
    SS_likes_nationality.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_likes_nationality.parquet")
    VP_parentCountry.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/VP_parentCountry.parquet")

    //L3
     SS_likes_subscribes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_likes_subscribes.parquet")

    //L4
    SS_tag_caption.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_tag_caption.parquet")
    SS_caption_tag.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_caption_tag.parquet")

    //L5
    SS_nationality_jobTitle.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_nationality_jobTitle.parquet")
    SS_jobTitle_nationality.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_jobTitle_nationality.parquet")


    //S1
    OS_offers_priceValidUntil.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/OS_offers_priceValidUntil.parquet")
    SS_validFrom_priceValidUntil.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_validFrom_priceValidUntil.parquet")
    SS_priceValidUntil_validFrom.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_priceValidUntil_validFrom.parquet")
    SS_validThrough_priceValidUntil.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_validThrough_priceValidUntil.parquet")
    SS_includes_priceValidUntil.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_includes_priceValidUntil.parquet")
    SS_price_priceValidUntil.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_price_priceValidUntil.parquet")
    SS_serialNumber_priceValidUntil.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_serialNumber_priceValidUntil.parquet")
    SS_eligibleQuantity_priceValidUntil.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_eligibleQuantity_priceValidUntil.parquet")
    SS_eligibleRegion_priceValidUntil.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_eligibleRegion_priceValidUntil.parquet")


    //S2
    SS_nationality_Location.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_nationality_Location.parquet")
    SS_type_nationality.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_type_nationality.parquet")
    SS_Location_nationality.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_Location_nationality.parquet")
    SS_gender_nationality.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_gender_nationality.parquet")


    //S3
    SS_type_publisher.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_type_publisher.parquet")
    SS_caption_publisher.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_caption_publisher.parquet")
    SS_publisher_caption.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_publisher_caption.parquet")
    SS_hasGenre_publisher.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_hasGenre_publisher.parquet")



    //S4
    SO_nationality_artist.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SO_nationality_artist.parquet")
    SO_age_artist.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SO_age_artist.parquet")
    OS_artist_nationality.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/OS_artist_nationality.parquet")
    SO_familyName_artist.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SO_familyName_artist.parquet")


    //S5
    SS_language_keywords.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_language_keywords.parquet")
    SS_type_language.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_type_language.parquet")
    SS_keywords_language.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_keywords_language.parquet")
    SS_description_language.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_description_language.parquet")

    //S6
    SS_hasGenre_conductor.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_hasGenre_conductor.parquet")
    VP_conductor.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/VP_conductor.parquet")
    SS_type_conductor.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_type_conductor.parquet")


    //S7
    OS_likes_text.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/OS_likes_text.parquet")
    SO_text_likes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SO_text_likes.parquet")
    SS_type_text.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/Parquet/SS_type_text.parquet")

    println("Parquet ExtVP partitioned and saved! Horizontal partitioning")

    }

  }
}
