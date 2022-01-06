package ee.ut.cs.bigdata.watdiv.partitioning.orc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ExtVerticalTablesPartition {
  def main(args: Array[String]): Unit = {

    println("Start Watdiv ExtVP Partitioning ORC...")

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench ORC ExtVP Partition")
      .getOrCreate()

    import spark.implicits._

    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase //value = {"Horizontal", "Subject", or "Predicate"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/"

    //read tables from HDFS

    //C1 FOR 10M STRUCTURE (WE NEED TO FIX THE PATH)
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
    SS_caption_hasReview.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_caption_hasReview.orc")
    SS_contentRating_caption.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_contentRating_caption.orc")
    SS_text_caption.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_text_caption.orc")
    SS_hasReview_caption.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_hasReview_caption.orc")
    SO_title_hasReview.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SO_title_hasReview.orc")
    SS_reviewer_title.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_reviewer_title.orc")
    SS_actor_language.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_actor_language.orc")
    SS_language_actor.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_language_actor.orc")


    //C2
    SO_eligibleRegion_offers.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SO_eligibleRegion_offers.orc")
    SS_offers_legalName.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_offers_legalName.orc")
    VP_LegalName.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/VP_LegalName.orc")
    OS_includes_hasReview.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/OS_includes_hasReview.orc")
    OS_hasReview_totalVotes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/OS_hasReview_totalVotes.orc")
    SO_totalVotes_hasReview.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SO_totalVotes_hasReview.orc")
    OS_purchaseFor_hasReview.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/OS_purchaseFor_hasReview.orc")
    SS_makesPurchase_homepage.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_makesPurchase_homepage.orc")
    SS_jobTitle_homepage.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_jobTitle_homepage.orc")
    SS_homepage_jobTitle.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_homepage_jobTitle.orc")


    //C3
    SS_Location_likes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_Location_likes.orc")
    SS_age_likes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_age_likes.orc")
    SS_gender_likes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_gender_likes.orc")
    SS_givenName_likes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_givenName_likes.orc")
    SS_likes_Location.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_likes_Location.orc")
    SS_friendOf_likes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_friendOf_likes.orc")

    //F1
    SO_tag_hasGenre.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SO_tag_hasGenre.orc")
    SO_type_hasGenre.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SO_type_hasGenre.orc")
    SS_hasGenre_trailer.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_hasGenre_trailer.orc")
    SS_type_trailer.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_type_trailer.orc")
    SS_keywords_trailer.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_keywords_trailer.orc")
    SS_trailer_keywords.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_trailer_keywords.orc")

    //F2
    SS_hasGenre_caption.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_hasGenre_caption.orc")
    SS_homepage_caption.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_homepage_caption.orc")
    SS_caption_homepage.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_caption_homepage.orc")
    SS_description_caption.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_description_caption.orc")
    SS_title_caption.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_title_caption.orc")
    SS_type_caption.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_type_caption.orc")
    SO_url_homepage.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SO_url_homepage.orc")
    SO_hits_homepage.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SO_hits_homepage.orc")

    //F3
    SS_hasGenre_contentSize.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_hasGenre_contentSize.orc")
    SS_contentRating_contentSize.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_contentRating_contentSize.orc")
    SS_contentSize_contentRating.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_contentSize_contentRating.orc")
    OS_purchaseFor_contentSize.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/OS_purchaseFor_contentSize.orc")
    OS_makesPurchase_purchaseDate.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/OS_makesPurchase_purchaseDate.orc")
    VP_purchaseDate.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/VP_purchaseDate.orc")

    //F4
    SO_language_homepage.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SO_language_homepage.orc")
    SS_homepage_contentSize.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_homepage_contentSize.orc")
    SS_tag_contentSize.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_tag_contentSize.orc")
    SS_contentSize_homepage.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_contentSize_homepage.orc")
    SS_description_contentSize.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_description_contentSize.orc")
    OS_includes_contentSize.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/OS_includes_contentSize.orc")
    OS_likes_contentSize.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/OS_likes_contentSize.orc")

    //F5
    OS_offers_validThrough.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/OS_offers_validThrough.orc")
    SO_validThrough_offers.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SO_validThrough_offers.orc")
    SS_includes_validThrough.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_includes_validThrough.orc")
    SO_title_includes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SO_title_includes.orc")
    SO_type_includes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SO_type_includes.orc")
    SS_price_validThrough.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_price_validThrough.orc")

    //L1
    SS_subscribes_likes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_subscribes_likes.orc")
    OS_likes_caption.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/OS_likes_caption.orc")
    SO_caption_likes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SO_caption_likes.orc")

    //L2
    SS_nationality_likes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_nationality_likes.orc")
    SS_likes_nationality.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_likes_nationality.orc")
    VP_parentCountry.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/VP_parentCountry.orc")

    //L3
     SS_likes_subscribes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_likes_subscribes.orc")

    //L4
    SS_tag_caption.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_tag_caption.orc")
    SS_caption_tag.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_caption_tag.orc")

    //L5
    SS_nationality_jobTitle.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_nationality_jobTitle.orc")
    SS_jobTitle_nationality.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_jobTitle_nationality.orc")


    //S1
    OS_offers_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/OS_offers_priceValidUntil.orc")
    SS_validFrom_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_validFrom_priceValidUntil.orc")
    SS_priceValidUntil_validFrom.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_priceValidUntil_validFrom.orc")
    SS_validThrough_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_validThrough_priceValidUntil.orc")
    SS_includes_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_includes_priceValidUntil.orc")
    SS_price_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_price_priceValidUntil.orc")
    SS_serialNumber_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_serialNumber_priceValidUntil.orc")
    SS_eligibleQuantity_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_eligibleQuantity_priceValidUntil.orc")
    SS_eligibleRegion_priceValidUntil.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_eligibleRegion_priceValidUntil.orc")


    //S2
    SS_nationality_Location.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_nationality_Location.orc")
    SS_type_nationality.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_type_nationality.orc")
    SS_Location_nationality.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_Location_nationality.orc")
    SS_gender_nationality.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_gender_nationality.orc")


    //S3
    SS_type_publisher.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_type_publisher.orc")
    SS_caption_publisher.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_caption_publisher.orc")
    SS_publisher_caption.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_publisher_caption.orc")
    SS_hasGenre_publisher.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_hasGenre_publisher.orc")



    //S4
    SO_nationality_artist.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SO_nationality_artist.orc")
    SO_age_artist.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SO_age_artist.orc")
    OS_artist_nationality.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/OS_artist_nationality.orc")
    SO_familyName_artist.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SO_familyName_artist.orc")


    //S5
    SS_language_keywords.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_language_keywords.orc")
    SS_type_language.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_type_language.orc")
    SS_keywords_language.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_keywords_language.orc")
    SS_description_language.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_description_language.orc")

    //S6
    SS_hasGenre_conductor.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_hasGenre_conductor.orc")
    VP_conductor.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/VP_conductor.orc")
    SS_type_conductor.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_type_conductor.orc")


    //S7
    OS_likes_text.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/OS_likes_text.orc")
    SO_text_likes.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SO_text_likes.orc")
    SS_type_text.repartition(84, $"Subject").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Subject/ORC/SS_type_text.orc")

      println("ORC ExtVP partitioned and saved! Subject based Partitioning")
    }

    else if (partitionType == "horizontal") {

    //C1
    SS_caption_hasReview.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_caption_hasReview.orc")
    SS_contentRating_caption.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_contentRating_caption.orc")
    SS_text_caption.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_text_caption.orc")
    SS_hasReview_caption.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_hasReview_caption.orc")
    SO_title_hasReview.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SO_title_hasReview.orc")
    SS_reviewer_title.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_reviewer_title.orc")
    SS_actor_language.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_actor_language.orc")
    SS_language_actor.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_language_actor.orc")


    //C2
    SO_eligibleRegion_offers.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SO_eligibleRegion_offers.orc")
    SS_offers_legalName.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_offers_legalName.orc")
    VP_LegalName.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/VP_LegalName.orc")
    OS_includes_hasReview.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/OS_includes_hasReview.orc")
    OS_hasReview_totalVotes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/OS_hasReview_totalVotes.orc")
    SO_totalVotes_hasReview.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SO_totalVotes_hasReview.orc")
    OS_purchaseFor_hasReview.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/OS_purchaseFor_hasReview.orc")
    SS_makesPurchase_homepage.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_makesPurchase_homepage.orc")
    SS_jobTitle_homepage.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_jobTitle_homepage.orc")
    SS_homepage_jobTitle.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_homepage_jobTitle.orc")


    //C3
    SS_Location_likes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_Location_likes.orc")
    SS_age_likes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_age_likes.orc")
    SS_gender_likes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_gender_likes.orc")
    SS_givenName_likes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_givenName_likes.orc")
    SS_likes_Location.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_likes_Location.orc")
    SS_friendOf_likes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_friendOf_likes.orc")

    //F1
    SO_tag_hasGenre.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SO_tag_hasGenre.orc")
    SO_type_hasGenre.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SO_type_hasGenre.orc")
    SS_hasGenre_trailer.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_hasGenre_trailer.orc")
    SS_type_trailer.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_type_trailer.orc")
    SS_keywords_trailer.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_keywords_trailer.orc")
    SS_trailer_keywords.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_trailer_keywords.orc")

    //F2
    SS_hasGenre_caption.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_hasGenre_caption.orc")
    SS_homepage_caption.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_homepage_caption.orc")
    SS_caption_homepage.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_caption_homepage.orc")
    SS_description_caption.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_description_caption.orc")
    SS_title_caption.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_title_caption.orc")
    SS_type_caption.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_type_caption.orc")
    SO_url_homepage.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SO_url_homepage.orc")
    SO_hits_homepage.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SO_hits_homepage.orc")

    //F3
    SS_hasGenre_contentSize.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_hasGenre_contentSize.orc")
    SS_contentRating_contentSize.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_contentRating_contentSize.orc")
    SS_contentSize_contentRating.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_contentSize_contentRating.orc")
    OS_purchaseFor_contentSize.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/OS_purchaseFor_contentSize.orc")
    OS_makesPurchase_purchaseDate.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/OS_makesPurchase_purchaseDate.orc")
    VP_purchaseDate.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/VP_purchaseDate.orc")

    //F4
    SO_language_homepage.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SO_language_homepage.orc")
    SS_homepage_contentSize.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_homepage_contentSize.orc")
    SS_tag_contentSize.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_tag_contentSize.orc")
    SS_contentSize_homepage.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_contentSize_homepage.orc")
    SS_description_contentSize.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_description_contentSize.orc")
    OS_includes_contentSize.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/OS_includes_contentSize.orc")
    OS_likes_contentSize.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/OS_likes_contentSize.orc")

    //F5
    OS_offers_validThrough.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/OS_offers_validThrough.orc")
    SO_validThrough_offers.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SO_validThrough_offers.orc")
    SS_includes_validThrough.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_includes_validThrough.orc")
    SO_title_includes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SO_title_includes.orc")
    SO_type_includes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SO_type_includes.orc")
    SS_price_validThrough.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_price_validThrough.orc")

    //L1
    SS_subscribes_likes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_subscribes_likes.orc")
    OS_likes_caption.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/OS_likes_caption.orc")
    SO_caption_likes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SO_caption_likes.orc")

    //L2
    SS_nationality_likes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_nationality_likes.orc")
    SS_likes_nationality.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_likes_nationality.orc")
    VP_parentCountry.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/VP_parentCountry.orc")

    //L3
     SS_likes_subscribes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_likes_subscribes.orc")

    //L4
    SS_tag_caption.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_tag_caption.orc")
    SS_caption_tag.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_caption_tag.orc")

    //L5
    SS_nationality_jobTitle.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_nationality_jobTitle.orc")
    SS_jobTitle_nationality.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_jobTitle_nationality.orc")


    //S1
    OS_offers_priceValidUntil.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/OS_offers_priceValidUntil.orc")
    SS_validFrom_priceValidUntil.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_validFrom_priceValidUntil.orc")
    SS_priceValidUntil_validFrom.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_priceValidUntil_validFrom.orc")
    SS_validThrough_priceValidUntil.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_validThrough_priceValidUntil.orc")
    SS_includes_priceValidUntil.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_includes_priceValidUntil.orc")
    SS_price_priceValidUntil.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_price_priceValidUntil.orc")
    SS_serialNumber_priceValidUntil.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_serialNumber_priceValidUntil.orc")
    SS_eligibleQuantity_priceValidUntil.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_eligibleQuantity_priceValidUntil.orc")
    SS_eligibleRegion_priceValidUntil.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_eligibleRegion_priceValidUntil.orc")


    //S2
    SS_nationality_Location.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_nationality_Location.orc")
    SS_type_nationality.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_type_nationality.orc")
    SS_Location_nationality.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_Location_nationality.orc")
    SS_gender_nationality.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_gender_nationality.orc")


    //S3
    SS_type_publisher.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_type_publisher.orc")
    SS_caption_publisher.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_caption_publisher.orc")
    SS_publisher_caption.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_publisher_caption.orc")
    SS_hasGenre_publisher.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_hasGenre_publisher.orc")



    //S4
    SO_nationality_artist.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SO_nationality_artist.orc")
    SO_age_artist.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SO_age_artist.orc")
    OS_artist_nationality.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/OS_artist_nationality.orc")
    SO_familyName_artist.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SO_familyName_artist.orc")


    //S5
    SS_language_keywords.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_language_keywords.orc")
    SS_type_language.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_type_language.orc")
    SS_keywords_language.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_keywords_language.orc")
    SS_description_language.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_description_language.orc")

    //S6
    SS_hasGenre_conductor.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_hasGenre_conductor.orc")
    VP_conductor.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/VP_conductor.orc")
    SS_type_conductor.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_type_conductor.orc")


    //S7
    OS_likes_text.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/OS_likes_text.orc")
    SO_text_likes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SO_text_likes.orc")
    SS_type_text.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path+ "ExtVP/Horizontal/ORC/SS_type_text.orc")

    println("ORC ExtVP partitioned and saved! Horizontal partitioning")

    }

  }
}
