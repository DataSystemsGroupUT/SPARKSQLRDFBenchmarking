package ee.ut.cs.bigdata.watdiv.querying.avro


import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.watdiv.querying.queries.ExtVPQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ExtVPTables {
  def main(args: Array[String]): Unit = {

    println("RDFBench WATDIV AVRO ExtVP")
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
    val SS_caption_hasReview = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/caption/hasReview.avro")
    val SS_contentRating_caption = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/contentRating/caption.avro")
    val SS_text_caption = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/sorg_text/caption.avro")
    val SS_hasReview_caption = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/hasReview/caption.avro")
    val SO_title_hasReview=spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SO/rev_title/hasReview.avro")
    val SS_reviewer_title=spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/reviewer/rev_title.avro")
    val SS_actor_language = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/actor/language.avro")
    val SS_language_actor = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/language/actor.avro")


    //C2 FOR 10M STRUCTURE
    val SO_eligibleRegion_offers = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SO/eligibleRegion/offers.avro")
    val SS_offers_legalName = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/offers/legalName.avro")
    val VP_LegalName = spark.read.format("avro").load(s"$path/VP/VHDFS/Avro/legalName.avro")
    val OS_includes_hasReview = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/OS/includes/hasReview.avro")
    val OS_hasReview_totalVotes = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/OS/hasReview/totalVotes.avro")
    val SO_totalVotes_hasReview = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SO/totalVotes/hasReview.avro")
    val OS_purchaseFor_hasReview = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/OS/purchaseFor/hasReview.avro")
    val SS_makesPurchase_homepage = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/makesPurchase/homepage.avro")
    val SS_jobTitle_homepage = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/jobTitle/homepage.avro")
    val SS_homepage_jobTitle = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/homepage/jobTitle.avro")

    //C3 FOR 10M STRUCTURE
    val SS_Location_likes = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/Location/likes.avro")
    val SS_age_likes = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/age/likes.avro")
    val SS_gender_likes = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/gender/likes.avro")
    val SS_givenName_likes = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/givenName/likes.avro")
    val SS_likes_Location = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/likes/Location.avro")
    val SS_friendOf_likes = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/friendOf/likes.avro")


    //F1 FOR 10M STRUCTURE
    val SO_tag_hasGenre = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SO/tag/hasGenre.avro")
    val SO_type_hasGenre = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SO/type/hasGenre.avro")
    val SS_hasGenre_trailer = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/hasGenre/trailer.avro")
    val SS_type_trailer = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/type/trailer.avro")
    val SS_keywords_trailer = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/keywords/trailer.avro")
    val SS_trailer_keywords = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/trailer/keywords.avro")

    //F2 FOR 10M STRUCTURE
    val SS_hasGenre_caption = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/hasGenre/caption.avro")
    val SS_homepage_caption = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/homepage/caption.avro")
    val SS_caption_homepage = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/caption/homepage.avro")
    val SS_description_caption = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/sorg_description/caption.avro")
    val SS_title_caption = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/og_title/caption.avro")
    val SS_type_caption = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/type/caption.avro")
    val SO_url_homepage = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SO/url/homepage.avro")
    val SO_hits_homepage = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SO/hits/homepage.avro")


    //F3 FOR 10M STRUCTURE
    val SS_hasGenre_contentSize = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/hasGenre/contentSize.avro")
    val SS_contentRating_contentSize = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/contentRating/contentSize.avro")
    val SS_contentSize_contentRating = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/contentSize/contentRating.avro")
    val OS_purchaseFor_contentSize = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/OS/purchaseFor/contentSize.avro")
    val OS_makesPurchase_purchaseDate = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/OS/makesPurchase/purchaseDate.avro")
    val VP_purchaseDate = spark.read.format("avro").load(s"$path/VP/VHDFS/Avro/purchaseDate.avro")


    //F4 FOR 10M STRUCTURE
    val SO_language_homepage = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SO/language/homepage.avro")
    val SS_homepage_contentSize = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/homepage/contentSize.avro")
    val SS_tag_contentSize = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/tag/contentSize.avro")
    val SS_contentSize_homepage = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/contentSize/homepage.avro")
    val SS_description_contentSize = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/sorg_description/contentSize.avro")
    val OS_includes_contentSize = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/OS/includes/contentSize.avro")
    val OS_likes_contentSize = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/OS/likes/contentSize.avro")

    //F5 FOR 10M STRUCTURE
    val OS_offers_validThrough = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/OS/offers/validThrough.avro")
    val SO_validThrough_offers = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SO/validThrough/offers.avro")
    val SS_includes_validThrough = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/includes/validThrough.avro")
    val SO_title_includes = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SO/og_title/includes.avro")
    val SO_type_includes = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SO/type/includes.avro")
    val SS_price_validThrough = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/price/validThrough.avro")


    //L1 FOR 10M STRUCTURE
    val SS_subscribes_likes = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/subscribes/likes.avro")
    val OS_likes_caption = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/OS/likes/caption.avro")
    val SO_caption_likes = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SO/caption/likes.avro")


    //L2 FOR 10M STRUCTURE
    val SS_nationality_likes = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/nationality/likes.avro")
    val SS_likes_nationality = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/likes/nationality.avro")
    val VP_parentCountry = spark.read.format("avro").load(s"$path/VP/VHDFS/Avro/parentCountry.avro")

    //L3 FOR 10M STRUCTURE
    val SS_likes_subscribes = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/likes/subscribes.avro")

    //L4 FOR 10M STRUCTURE
    val SS_tag_caption = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/tag/caption.avro")
    val SS_caption_tag = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/caption/tag.avro")

    //L5 FOR 10M STRUCTURE
    val SS_nationality_jobTitle = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/nationality/jobTitle.avro")
    val SS_jobTitle_nationality = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/jobTitle/nationality.avro")

    //S1
    val OS_offers_priceValidUntil = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/OS/offers/priceValidUntil.avro")
    val SS_validFrom_priceValidUntil = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/validFrom/priceValidUntil.avro")
    val SS_priceValidUntil_validFrom = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/priceValidUntil/validFrom.avro")
    val SS_validThrough_priceValidUntil = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/validThrough/priceValidUntil.avro")
    val SS_includes_priceValidUntil = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/includes/priceValidUntil.avro")
    val SS_price_priceValidUntil = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/price/priceValidUntil.avro")
    val SS_serialNumber_priceValidUntil = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/serialNumber/priceValidUntil.avro")
    val SS_eligibleQuantity_priceValidUntil = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/eligibleQuantity/priceValidUntil.avro")
    val SS_eligibleRegion_priceValidUntil = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/eligibleRegion/priceValidUntil.avro")

    //S2
    val SS_nationality_Location = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/nationality/Location.avro")
    val SS_type_nationality = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/type/Location.avro")
    val SS_Location_nationality = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/Location/nationality.avro")
    val SS_gender_nationality = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/gender/nationality.avro")

    //S3
    val SS_type_publisher = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/type/publisher.avro")
    val SS_caption_publisher = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/caption/publisher.avro")
    val SS_publisher_caption = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/publisher/caption.avro")
    val SS_hasGenre_publisher = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/hasGenre/publisher.avro")

    //S4
    val SO_nationality_artist = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SO/nationality/artist.avro")
    val SO_age_artist = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SO/age/artist.avro")
    val OS_artist_nationality = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/OS/artist/nationality.avro")
    val SO_familyName_artist = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SO/familyName/artist.avro")


    //S5
    val SS_language_keywords = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/language/keywords.avro")
    val SS_type_language = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/type/language.avro")
    val SS_keywords_language = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/keywords/language.avro")
    val SS_description_language = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/sorg_description/language.avro")

    //S6
    val SS_hasGenre_conductor = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/hasGenre/conductor.avro")
    val VP_conductor = spark.read.format("avro").load(s"$path/VP/VHDFS/Avro/conductor.avro")
    val SS_type_conductor = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/type/conductor.avro")


    //S7
    val OS_likes_text = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/OS/likes/sorg_text.avro")
    val SO_text_likes = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SO/sorg_text/likes.avro")
    val SS_type_text = spark.read.format("avro").load(s"$path/ExtVP/VHDFS/Avro/SS/type/sorg_text.avro")


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
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/watdiv/$ds/Avro/ExtVP/VHDFS$ds.txt"),true)

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
    println("All Queries are Done - Avro - VP!")
  }

}
