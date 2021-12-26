package ee.ut.cs.bigdata.watdiv.querying.parquet


import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.watdiv.querying.queries.ExtVPQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ExtVPTables {
  def main(args: Array[String]): Unit = {

    println("RDFBench WATDIV PARQUET ExtVP")
    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench WATDIV AVRO ExtVP")
      .getOrCreate()
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/"

    //read tables from HDFS

    //C1 FOR 10M STRUCTURE (WE NEED TO FIX THE PATH)
    val SS_caption_hasReview = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/caption/hasReview.parquet")
    val SS_contentRating_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/contentRating/caption.parquet")
    val SS_text_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/text/caption.parquet")
    val SS_hasReview_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/hasReview/caption.parquet")
    val SO_title_hasReview=spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/")
    //INSTEAD WE USE The following (VP	<rev__title>)
    val VP_Rev_title = spark.read.format("parquet").load(s"$path/VP/Parquet/rev_title.parquet")
    val SS_reviewer_title=spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/reviewer/rev_title.parquet")
    //INSTEAD WE USE The following <rev__reviewer>)
//    val VP_Reviewer = spark.read.format("parquet").load(s"$path/VP/Parquet/reviewer.parquet")
    val SS_actor_language = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/actor/language.parquet")
    val SS_language_actor = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/language/actor.parquet")


    //C2 FOR 10M STRUCTURE
    val SO_eligibleRegion_offers = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/eligibleRegion/offers.parquet")
    val SS_offers_legalName = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/offers/legalName.parquet")
    val VP_LegalName = spark.read.format("parquet").load(s"$path/VP/Parquet/legalName.parquet")
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
    val SS_description_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/description/caption.parquet")
    val SS_title_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/rev_title/caption.parquet")
    val SS_type_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/type/caption.parquet")
    val SO_url_homepage = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/url/homepage.parquet")
    val SO_hits_homepage = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/hits/homepage.parquet")


    //F3 FOR 10M STRUCTURE
    val SS_hasGenre_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/hasGenre/contentSize.parquet")
    val SS_contentRating_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/contentRating/contentSize.parquet")
    val SS_contentSize_contentRating = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/contentSize/contentRating.parquet")
    val OS_purchaseFor_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/purchaseFor/contentSize.parquet")
    val OS_makesPurchase_purchaseDate = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/makesPurchase/purchaseDate.parquet")
    //val SO_purchaseDate_makesPurchase = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/makesPurchase/purchaseDate.parquet") // *********NOT FOUND in 10M **********
    //INSTEAD WE USE The following <VP_purchaseDate>)
    val VP_purchaseDate = spark.read.format("parquet").load(s"$path/VP/Parquet/purchaseDate.parquet")


    //F4 FOR 10M STRUCTURE
    val SO_language_homepage = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/language/homepage.parquet")
    val SS_homepage_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/homepage/contentSize.parquet")
    val SS_tag_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/tag/contentSize.parquet")
    val SS_contentSize_homepage = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/contentSize/homepage.parquet")
    val SS_description_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/description/contentSize.parquet")
    val OS_includes_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/includes/contentSize.parquet")
    val OS_likes_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/likes/contentSize.parquet")

    //F5 FOR 10M STRUCTURE
    val OS_offers_validThrough = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/offers/validThrough.parquet")
    val SO_validThrough_offers = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/validThrough/offers.parquet")
    val SS_includes_validThrough = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/includes/validThrough.parquet")
    val SO_title_includes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/title/includes.parquet")
    val SO_type_includes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/type/includes.parquet")
    val SS_price_validThrough = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/price/validThrough.parquet")


    //L1 FOR 10M STRUCTURE
    val SS_subscribes_likes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/subscribes/likes.parquet")
    val OS_likes_caption = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/likes/caption.parquet")
    val SO_caption_likes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/caption/likes.parquet")


    //L2 FOR 10M STRUCTURE
    val SS_nationality_likes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/nationality/likes.parquet")
    val SS_likes_nationality = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/likes/nationality.parquet")
    val VP_parentCountry = spark.read.format("parquet").load(s"$path/VP/Parquet/parentCountry.parquet")

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
    val SS_description_language = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/description/language.parquet")

    //S6
    val SS_hasGenre_conductor = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/hasGenre/conductor.parquet")
    val VP_conductor = spark.read.format("parquet").load(s"$path/VP/Parquet/conductor.parquet")
    val SS_type_conductor = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/type/conductor.parquet")

    //S7
    val OS_likes_text = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/OS/likes/text.parquet")
    val SO_text_likes = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SO/text/likes.parquet")
    val SS_type_text = spark.read.format("parquet").load(s"$path/ExtVP/VHDFS/Parquet/SS/type/text.parquet")


    //C1
    SS_caption_hasReview.createOrReplaceTempView("SS_caption_hasReview")
    SS_contentRating_caption.createOrReplaceTempView("SS_contentRating_caption")
    SS_text_caption.createOrReplaceTempView("SS_text_caption")
    SS_hasReview_caption.createOrReplaceTempView("SS_hasReview_caption")
    VP_Rev_title.createOrReplaceTempView("VP_Rev_title")
    SS_reviewer_title.createOrReplaceTempView("SS_reviewer_title")
    SS_actor_language.createOrReplaceTempView("SS_actor_language")
    SS_language_actor.createOrReplaceTempView("SS_language_actor")


    //C2
    SO_eligibleRegion_offers.createOrReplaceTempView("SO_eligibleRegion_offers")
    SS_offers_legalName.createOrReplaceTempView("SS_offers_legalName")
    VP_LegalName.createOrReplaceTempView("VP_LegalName")
    OS_includes_hasReview.createOrReplaceTempView("OS_includes_hasReview")
    OS_hasReview_totalVotes.createOrReplaceTempView("OS_hasReview_totalVotes")
    SO_totalVotes_hasReview.createOrReplaceTempView("SO_totalVotes_hasReview")
    OS_purchaseFor_hasReview.createOrReplaceTempView("OS_purchaseFor_hasReview")
    SS_makesPurchase_homepage.createOrReplaceTempView("SS_makesPurchase_homepage")
    SS_jobTitle_homepage.createOrReplaceTempView("SS_jobTitle_homepage")
    SS_homepage_jobTitle.createOrReplaceTempView("SS_homepage_jobTitle")


    //C3
    SS_Location_likes.createOrReplaceTempView("SS_Location_likes")
    SS_age_likes.createOrReplaceTempView("SS_age_likes")
    SS_gender_likes.createOrReplaceTempView("SS_gender_likes")
    SS_givenName_likes.createOrReplaceTempView("SS_givenName_likes")
    SS_likes_Location.createOrReplaceTempView("SS_likes_Location")
    SS_friendOf_likes.createOrReplaceTempView("SS_friendOf_likes")

    //F1
    SO_tag_hasGenre.createOrReplaceTempView("SO_tag_hasGenre")
    SO_type_hasGenre.createOrReplaceTempView("SO_type_hasGenre")
    SS_hasGenre_trailer.createOrReplaceTempView("SS_hasGenre_trailer")
    SS_type_trailer.createOrReplaceTempView("SS_type_trailer")
    SS_keywords_trailer.createOrReplaceTempView("SS_keywords_trailer")
    SS_trailer_keywords.createOrReplaceTempView("SS_trailer_keywords")

    //F2
    SS_hasGenre_caption.createOrReplaceTempView("SS_hasGenre_caption")
    SS_homepage_caption.createOrReplaceTempView("SS_homepage_caption")
    SS_caption_homepage.createOrReplaceTempView("SS_caption_homepage")
    SS_description_caption.createOrReplaceTempView("SS_description_caption")
    SS_title_caption.createOrReplaceTempView("SS_title_caption")
    SS_type_caption.createOrReplaceTempView("SS_type_caption")
    SO_url_homepage.createOrReplaceTempView("SO_url_homepage")
    SO_hits_homepage.createOrReplaceTempView("SO_hits_homepage")

    //F3
    SS_hasGenre_contentSize.createOrReplaceTempView("SS_hasGenre_contentSize")
    SS_contentRating_contentSize.createOrReplaceTempView("SS_contentRating_contentSize")
    SS_contentSize_contentRating.createOrReplaceTempView("SS_contentSize_contentRating")
    OS_purchaseFor_contentSize.createOrReplaceTempView("OS_purchaseFor_contentSize")
    OS_makesPurchase_purchaseDate.createOrReplaceTempView("OS_makesPurchase_purchaseDate")
    // SO_purchaseDate_makesPurchase.createOrReplaceTempView("SO_purchaseDate_makesPurchase") =
    //INSTEAD WE USE The following <VP_purchaseDate>)
    VP_purchaseDate.createOrReplaceTempView("VP_purchaseDate")

    //F4
    SO_language_homepage.createOrReplaceTempView("SO_language_homepage")
    SS_homepage_contentSize.createOrReplaceTempView("SS_homepage_contentSize")
    SS_tag_contentSize.createOrReplaceTempView("SS_tag_contentSize")
    SS_contentSize_homepage.createOrReplaceTempView("SS_contentSize_homepage")
    SS_description_contentSize.createOrReplaceTempView("SS_description_contentSize")
    OS_includes_contentSize.createOrReplaceTempView("OS_includes_contentSize")
    OS_likes_contentSize.createOrReplaceTempView("OS_likes_contentSize")

    //F5
    OS_offers_validThrough.createOrReplaceTempView("OS_offers_validThrough")
    SO_validThrough_offers.createOrReplaceTempView("SO_validThrough_offers")
    SS_includes_validThrough.createOrReplaceTempView("SS_includes_validThrough")
    SO_title_includes.createOrReplaceTempView("SO_title_includes")
    SO_type_includes.createOrReplaceTempView("SO_type_includes")
    SS_price_validThrough.createOrReplaceTempView("SS_price_validThrough")

    //L1
    SS_subscribes_likes.createOrReplaceTempView("SS_subscribes_likes")
    OS_likes_caption.createOrReplaceTempView("OS_likes_caption")
    SO_caption_likes.createOrReplaceTempView("SO_caption_likes")

    //L2
    SS_nationality_likes.createOrReplaceTempView("SS_nationality_likes")
    SS_likes_nationality.createOrReplaceTempView("SS_likes_nationality")
    VP_parentCountry.createOrReplaceTempView("VP_parentCountry")

    //L3
     SS_likes_subscribes.createOrReplaceTempView("SS_likes_subscribes")

    //L4
    SS_tag_caption.createOrReplaceTempView("SS_tag_caption")
    SS_caption_tag.createOrReplaceTempView("SS_caption_tag")

    //L5
    SS_nationality_jobTitle.createOrReplaceTempView("SS_nationality_jobTitle")
    SS_jobTitle_nationality.createOrReplaceTempView("SS_jobTitle_nationality")


    //S1
    OS_offers_priceValidUntil.createOrReplaceTempView("OS_offers_priceValidUntil")
    SS_validFrom_priceValidUntil.createOrReplaceTempView("SS_validFrom_priceValidUntil")
    SS_priceValidUntil_validFrom.createOrReplaceTempView("SS_priceValidUntil_validFrom")
    SS_validThrough_priceValidUntil.createOrReplaceTempView("SS_validThrough_priceValidUntil")
    SS_includes_priceValidUntil.createOrReplaceTempView("SS_includes_priceValidUntil")
    SS_price_priceValidUntil.createOrReplaceTempView("SS_price_priceValidUntil")
    SS_serialNumber_priceValidUntil.createOrReplaceTempView("SS_serialNumber_priceValidUntil")
    SS_eligibleQuantity_priceValidUntil.createOrReplaceTempView("SS_eligibleQuantity_priceValidUntil")
    SS_eligibleRegion_priceValidUntil.createOrReplaceTempView("SS_eligibleRegion_priceValidUntil")


    //S2
    SS_nationality_Location.createOrReplaceTempView("SS_nationality_Location")
    SS_type_nationality.createOrReplaceTempView("SS_type_nationality")
    SS_Location_nationality.createOrReplaceTempView("SS_Location_nationality")
    SS_gender_nationality.createOrReplaceTempView("SS_gender_nationality")


    //S3
    SS_type_publisher.createOrReplaceTempView("SS_type_publisher")
    SS_caption_publisher.createOrReplaceTempView("SS_caption_publisher")
    SS_publisher_caption.createOrReplaceTempView("SS_publisher_caption")
    SS_hasGenre_publisher.createOrReplaceTempView("SS_hasGenre_publisher")

    //S4
    SO_nationality_artist.createOrReplaceTempView("SO_nationality_artist")
    SO_age_artist.createOrReplaceTempView("SO_age_artist")
    OS_artist_nationality.createOrReplaceTempView("OS_artist_nationality")
    SO_familyName_artist.createOrReplaceTempView("SO_familyName_artist")

    //S5
    SS_language_keywords.createOrReplaceTempView("SS_language_keywords")
    SS_type_language.createOrReplaceTempView("SS_type_language")
    SS_keywords_language.createOrReplaceTempView("SS_keywords_language")
    SS_description_language.createOrReplaceTempView("SS_description_language")

    //S6
    SS_hasGenre_conductor.createOrReplaceTempView("SS_hasGenre_conductor")
    VP_conductor.createOrReplaceTempView("VP_conductor")
    SS_type_conductor.createOrReplaceTempView("SS_type_conductor")


    //S7
    OS_likes_text.createOrReplaceTempView("OS_likes_text")
    SO_text_likes.createOrReplaceTempView("SO_text_likes")
    SS_type_text.createOrReplaceTempView("SS_type_text")


    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/parquet/ExtVP/VHDFS$ds.txt"),true)

    val queries = List(
      new ExtVPQueries C1, new ExtVPQueries C2, new ExtVPQueries C3,
      new ExtVPQueries F1, new ExtVPQueries F2, new ExtVPQueries F3, new ExtVPQueries F4, new ExtVPQueries F5,
      new ExtVPQueries L1, new ExtVPQueries L2, new ExtVPQueries L3,new ExtVPQueries L4, new ExtVPQueries L5,
      new ExtVPQueries S1, new ExtVPQueries S2, new ExtVPQueries S3,new ExtVPQueries S4, new ExtVPQueries S5,new ExtVPQueries S6, new ExtVPQueries S7
    )


    var count = 1
    for (query <- queries) {
      //run query and calculate the run time
      val startTime = System.nanoTime()
      val df_count = spark.sql(query).count()
      println(df_count)
      //df.take(100).foreach(println)
      val endTime = System.nanoTime()
      val result = (endTime - startTime).toDouble / 1000000000
      count += 1
    }
    println("All Queries are Done - Parquet - VP!")
  }
}
