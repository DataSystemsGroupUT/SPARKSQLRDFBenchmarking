package ee.ut.cs.bigdata.watdiv.querying.orc


import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.watdiv.querying.queries.ExtVPQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ExtVPTables {
  def main(args: Array[String]): Unit = {

    println("RDFBench WATDIV ORC ExtVP")
    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench WATDIV ORC ExtVP")
      .getOrCreate()
    val ds = args(0) //value = {"100M", "500M, or "1B"}
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
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/watdiv/$ds/orc/ExtVP/VHDFS$ds.txt"),true)

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
    println("All Queries are Done - ORC - VP!")
  }

}
