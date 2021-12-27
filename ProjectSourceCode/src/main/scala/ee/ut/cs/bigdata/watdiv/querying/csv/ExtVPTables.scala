package ee.ut.cs.bigdata.watdiv.querying.csv


import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.watdiv.querying.queries.ExtVPQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ExtVPTables {
  def main(args: Array[String]): Unit = {

    println("RDFBench WATDIV CSV ExtVP")
    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench WATDIV CSV ExtVP")
      .getOrCreate()
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/"

    //read tables from HDFS

    /*
    //C1 FOR 10M STRUCTURE (WE NEED TO FIX THE PATH)
    val SS_caption_hasReview = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/caption/hasReview.csv")
    val SS_contentRating_caption = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/contentRating/caption.csv")
    val SS_text_caption = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/sorg_text/caption.csv")
    val SS_hasReview_caption = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/hasReview/caption.csv")
    val SO_title_hasReview=spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SO/rev_title/hasReview.csv")
    //INSTEAD WE USE The following (VP	<rev__title>)
//    val VP_Rev_title = spark.read.format("csv").option("header", true).load(s"$path/VP/VHDFS/CSV/rev_title.csv")
    val SS_reviewer_title=spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/reviewer/rev_title.csv")
    //INSTEAD WE USE The following <rev__reviewer>)
//    val VP_Reviewer = spark.read.format("csv").option("header", true).load(s"$path/VP/VHDFS/CSV/reviewer.csv")
    val SS_actor_language = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/actor/language.csv")
    val SS_language_actor = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/language/actor.csv")


    //C2 FOR 10M STRUCTURE
    val SO_eligibleRegion_offers = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SO/eligibleRegion/offers.csv")
    val SS_offers_legalName = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/offers/legalName.csv")
    val VP_LegalName = spark.read.format("csv").option("header", true).load(s"$path/VP/VHDFS/CSV/legalName.csv")
    val OS_includes_hasReview = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/OS/includes/hasReview.csv")
    val OS_hasReview_totalVotes = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/OS/hasReview/totalVotes.csv")
    val SO_totalVotes_hasReview = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SO/totalVotes/hasReview.csv")
    val OS_purchaseFor_hasReview = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/OS/purchaseFor/hasReview.csv")
    val SS_makesPurchase_homepage = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/makesPurchase/homepage.csv")
    val SS_jobTitle_homepage = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/jobTitle/homepage.csv")
    val SS_homepage_jobTitle = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/homepage/jobTitle.csv")

    //C3 FOR 10M STRUCTURE
    val SS_Location_likes = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/Location/likes.csv")
    val SS_age_likes = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/age/likes.csv")
    val SS_gender_likes = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/gender/likes.csv")
    val SS_givenName_likes = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/givenName/likes.csv")
    val SS_likes_Location = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/likes/Location.csv")
    val SS_friendOf_likes = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/friendOf/likes.csv")


    //F1 FOR 10M STRUCTURE
    val SO_tag_hasGenre = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SO/tag/hasGenre.csv")
    val SO_type_hasGenre = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SO/type/hasGenre.csv")
    val SS_hasGenre_trailer = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/hasGenre/trailer.csv")
    val SS_type_trailer = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/type/trailer.csv")
    val SS_keywords_trailer = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/keywords/trailer.csv")
    val SS_trailer_keywords = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/trailer/keywords.csv")

    //F2 FOR 10M STRUCTURE
    val SS_hasGenre_caption = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/hasGenre/caption.csv")
    val SS_homepage_caption = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/homepage/caption.csv")
    val SS_caption_homepage = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/caption/homepage.csv")
    val SS_description_caption = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/sorg_description/caption.csv")
    val SS_title_caption = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/og_title/caption.csv")
    val SS_type_caption = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/type/caption.csv")
    val SO_url_homepage = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SO/url/homepage.csv")
    val SO_hits_homepage = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SO/hits/homepage.csv")


    //F3 FOR 10M STRUCTURE
    val SS_hasGenre_contentSize = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/hasGenre/contentSize.csv")
    val SS_contentRating_contentSize = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/contentRating/contentSize.csv")
    val SS_contentSize_contentRating = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/contentSize/contentRating.csv")
    val OS_purchaseFor_contentSize = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/OS/purchaseFor/contentSize.csv")
    val OS_makesPurchase_purchaseDate = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/OS/makesPurchase/purchaseDate.csv")
    //val SO_purchaseDate_makesPurchase = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/OS/makesPurchase/purchaseDate.csv") // *********NOT FOUND in 10M **********
    //INSTEAD WE USE The following <VP_purchaseDate>)
    val VP_purchaseDate = spark.read.format("csv").option("header", true).load(s"$path/VP/VHDFS/CSV/purchaseDate.csv")


    //F4 FOR 10M STRUCTURE
    val SO_language_homepage = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SO/language/homepage.csv")
    val SS_homepage_contentSize = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/homepage/contentSize.csv")
    val SS_tag_contentSize = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/tag/contentSize.csv")
    val SS_contentSize_homepage = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/contentSize/homepage.csv")
    val SS_description_contentSize = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/sorg_description/contentSize.csv")
    val OS_includes_contentSize = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/OS/includes/contentSize.csv")
    val OS_likes_contentSize = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/OS/likes/contentSize.csv")

    //F5 FOR 10M STRUCTURE
    val OS_offers_validThrough = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/OS/offers/validThrough.csv")
    val SO_validThrough_offers = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SO/validThrough/offers.csv")
    val SS_includes_validThrough = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/includes/validThrough.csv")
    val SO_title_includes = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SO/og_title/includes.csv")
    val SO_type_includes = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SO/type/includes.csv")
    val SS_price_validThrough = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/price/validThrough.csv")


    //L1 FOR 10M STRUCTURE
    val SS_subscribes_likes = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/subscribes/likes.csv")
    val OS_likes_caption = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/OS/likes/caption.csv")
    val SO_caption_likes = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SO/caption/likes.csv")


    //L2 FOR 10M STRUCTURE
    val SS_nationality_likes = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/nationality/likes.csv")
    val SS_likes_nationality = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/likes/nationality.csv")
    val VP_parentCountry = spark.read.format("csv").option("header", true).load(s"$path/VP/VHDFS/CSV/parentCountry.csv")

    //L3 FOR 10M STRUCTURE
    val SS_likes_subscribes = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/likes/subscribes.csv")

    //L4 FOR 10M STRUCTURE
    val SS_tag_caption = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/tag/caption.csv")
    val SS_caption_tag = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/caption/tag.csv")

    //L5 FOR 10M STRUCTURE
    val SS_nationality_jobTitle = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/nationality/jobTitle.csv")
    val SS_jobTitle_nationality = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/jobTitle/nationality.csv")

    //S1
    val OS_offers_priceValidUntil = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/OS/offers/priceValidUntil.csv")
    val SS_validFrom_priceValidUntil = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/validFrom/priceValidUntil.csv")
    val SS_priceValidUntil_validFrom = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/priceValidUntil/validFrom.csv")
    val SS_validThrough_priceValidUntil = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/validThrough/priceValidUntil.csv")
    val SS_includes_priceValidUntil = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/includes/priceValidUntil.csv")
    val SS_price_priceValidUntil = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/price/priceValidUntil.csv")
    val SS_serialNumber_priceValidUntil = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/serialNumber/priceValidUntil.csv")
    val SS_eligibleQuantity_priceValidUntil = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/eligibleQuantity/priceValidUntil.csv")
    val SS_eligibleRegion_priceValidUntil = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/eligibleRegion/priceValidUntil.csv")

    //S2
    val SS_nationality_Location = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/nationality/Location.csv")
    val SS_type_nationality = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/type/Location.csv")
    val SS_Location_nationality = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/Location/nationality.csv")
    val SS_gender_nationality = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/gender/nationality.csv")

    //S3
    val SS_type_publisher = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/type/publisher.csv")
    val SS_caption_publisher = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/caption/publisher.csv")
    val SS_publisher_caption = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/publisher/caption.csv")
    val SS_hasGenre_publisher = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/hasGenre/publisher.csv")
*/
    //S4
    val SO_nationality_artist = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SO/nationality/artist.csv")
    val SO_age_artist = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SO/age/artist.csv")
    val OS_artist_nationality = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/OS/artist/nationality.csv")
    val SO_familyName_artist = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SO/familyName/artist.csv")

    /*
    //S5
    val SS_language_keywords = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/language/keywords.csv")
    val SS_type_language = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/type/language.csv")
    val SS_keywords_language = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/keywords/language.csv")
    val SS_description_language = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/sorg_description/language.csv")

    //S6
    val SS_hasGenre_conductor = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/hasGenre/conductor.csv")
    val VP_conductor = spark.read.format("csv").option("header", true).load(s"$path/VP/VHDFS/CSV/conductor.csv")
    val SS_type_conductor = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/type/conductor.csv")


    //S7
    val OS_likes_text = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/OS/likes/sorg_text.csv")
    val SO_text_likes = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SO/sorg_text/likes.csv")
    val SS_type_text = spark.read.format("csv").option("header", true).load(s"$path/ExtVP/VHDFS/CSV/SS/type/sorg_text.csv")
    */

    /*
    //C1
    SS_caption_hasReview.createOrReplaceTempView("SS_caption_hasReview")
    SS_contentRating_caption.createOrReplaceTempView("SS_contentRating_caption")
    SS_text_caption.createOrReplaceTempView("SS_text_caption")
    SS_hasReview_caption.createOrReplaceTempView("SS_hasReview_caption")
//    VP_Rev_title.createOrReplaceTempView("VP_Rev_title")
    SO_title_hasReview.createOrReplaceTempView("SO_title_hasReview")
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

    */

    //S4
    SO_nationality_artist.createOrReplaceTempView("SO_nationality_artist")
    SO_age_artist.createOrReplaceTempView("SO_age_artist")
    OS_artist_nationality.createOrReplaceTempView("OS_artist_nationality")
    SO_familyName_artist.createOrReplaceTempView("SO_familyName_artist")

/*
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
    */

    //create file to write the query run time results
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/watdiv/$ds/csv/ExtVP/VHDFS$ds.txt"),true)

    val queries = List(
//      new ExtVPQueries C1, new ExtVPQueries C2, new ExtVPQueries C3,
//      new ExtVPQueries F1, new ExtVPQueries F2, new ExtVPQueries F3, new ExtVPQueries F4, new ExtVPQueries F5,
//      new ExtVPQueries L1, new ExtVPQueries L2, new ExtVPQueries L3,new ExtVPQueries L4, new ExtVPQueries L5,
//      new ExtVPQueries S1, new ExtVPQueries S2, new ExtVPQueries S3,new ExtVPQueries S4, new ExtVPQueries S5,new ExtVPQueries S6, new ExtVPQueries S7
      new ExtVPQueries S4
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
    println("All Queries are Done - CSV - VP!")
  }
}
