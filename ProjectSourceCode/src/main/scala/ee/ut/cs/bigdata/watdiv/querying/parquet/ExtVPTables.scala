package ee.ut.cs.bigdata.watdiv.querying.parquet


import java.io.{File, FileOutputStream}
import ee.ut.cs.bigdata.watdiv.querying.queries.ExtVPQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ExtVPTables {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Parquet PT")
      .getOrCreate()
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/"

    //read tables from HDFS


    //C1 FOR 10M STRUCTURE (WE NEED TO FIX THE PATH)
    val SS_caption_hasReview = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/caption/hasReview.parquet")
    val SS_contentRating_caption = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/contentRating/caption.parquet")
    val SS_text_caption = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/text/caption.parquet")
    val SS_hasReview_caption = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/hasReview/caption.parquet")
    //val SO_title_hasReview=spark.read.format("parquet").load(s"$path/ExtVP/Parquet/") *********NOT FOUND in 10M **********
    //INSTEAD WE USE The following (VP	<rev__title>)
    val VP_Rev_title = spark.read.format("parquet").load(s"$path/VP/Parquet/rev_title.parquet")
    //val SS_reviewer_title=spark.read.format("parquet").load(s"$path/ExtVP/Parquet/") *********NOT FOUND in 10M **********
    //INSTEAD WE USE The following <rev__reviewer>)
    val VP_Reviewer = spark.read.format("parquet").load(s"$path/VP/Parquet/reviewer.parquet")
    val SS_actor_language = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/actor/language.parquet")
    val SS_language_actor = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/language/actor.parquet")


    //C2 FOR 10M STRUCTURE
    val SO_eligibleRegion_offers = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SO/eligibleRegion/offers.parquet")
    val SS_offers_legalName = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/offers/legalName.parquet")
    val VP_LegalName = spark.read.format("parquet").load(s"$path/VP/Parquet/legalName.parquet")
    val OS_includes_hasReview = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/OS/includes/hasReview.parquet")
    val OS_hasReview_totalVotes = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/OS/hasReview/totalVotes.parquet")
    val SO_totalVotes_hasReview = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SO/totalVotes/hasReview.parquet")
    val OS_purchaseFor_hasReview = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/OS/purchaseFor/hasReview.parquet")
    val SS_makesPurchase_homepage = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/makesPurchase/homepage.parquet")
    val SS_jobTitle_homepage = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/jobTitle/homepage.parquet")
    val SS_homepage_jobTitle = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/homepage/jobTitle.parquet")

    //C3 FOR 10M STRUCTURE
    val SS_Location_likes = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/Location/likes.parquet")
    val SS_age_likes = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/age/likes.parquet")
    val SS_gender_likes = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/gender/likes.parquet")
    val SS_givenName_likes = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/givenName/likes.parquet")
    val SS_likes_Location = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/likes/Location.parquet")
    val SS_friendOf_likes = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/friendOf/likes.parquet")


    //F1 FOR 10M STRUCTURE
    val SO_tag_hasGenre = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SO/tag/hasGenre.parquet")
    val SO_type_hasGenre = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SO/type/hasGenre.parquet")
    val SS_hasGenre_trailer = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/hasGenre/trailer.parquet")
    val SS_type_trailer = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/trailer.parquet")
    val SS_keywords_trailer = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/keywords/trailer.parquet")
    val SS_trailer_keywords = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/trailer/keywords.parquet")

    //F2 FOR 10M STRUCTURE
    val SS_hasGenre_caption = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/hasGenre/caption.parquet")
    val SS_homepage_caption = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/homepage/caption.parquet")
    val SS_caption_homepage = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/caption/homepage.parquet")
    val SS_description_caption = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/description/caption.parquet")
    val SS_title_caption = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/title/caption.parquet")
    val SS_type_caption = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/type/caption.parquet")
    val SO_url_homepage = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SO/url/homepage.parquet")
    val SO_hits_homepage = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SO/hits/homepage.parquet")


    //F3 FOR 10M STRUCTURE
    val SS_hasGenre_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/hasGenre/contentSize.parquet")
    val SS_contentRating_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/contentRating/contentSize.parquet")
    val SS_contentSize_contentRating = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/contentSize/contentRating.parquet")
    val OS_purchaseFor_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/OS/purchaseFor/contentSize.parquet")
    val OS_makesPurchase_purchaseDate = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/OS/makesPurchase/purchaseDate.parquet")
    //val SO_purchaseDate_makesPurchase = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/OS/makesPurchase/purchaseDate.parquet") // *********NOT FOUND in 10M **********
    //INSTEAD WE USE The following <VP_purchaseDate>)
    val VP_purchaseDate = spark.read.format("parquet").load(s"$path/VP/Parquet/purchaseDate.parquet")


    //F4 FOR 10M STRUCTURE
    val SO_language_homepage = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SO/language/homepage.parquet")
    val SS_homepage_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/homepage/contentSize.parquet")
    val SS_tag_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/tag/contentSize.parquet")
    val SS_contentSize_homepage = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/contentSize/homepage.parquet")
    val SS_description_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/description/contentSize.parquet")
    val OS_includes_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/OS/includes/contentSize.parquet")
    val OS_likes_contentSize = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/OS/likes/contentSize.parquet")

    //F5 FOR 10M STRUCTURE
    val OS_offers_validThrough = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/OS/offers/validThrough.parquet")
    val SO_validThrough_offers = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SO/validThrough/offers.parquet")
    val SS_includes_validThrough = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/includes/validThrough.parquet")
    val SO_title_includes = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SO/title/includes.parquet")
    val SO_type_includes = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SO/type/includes.parquet")
    val SS_price_validThrough = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/price/validThrough.parquet")


    //L1 FOR 10M STRUCTURE
    val SS_subscribes_likes = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/subscribes/likes.parquet")
    val OS_likes_caption = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/OS/likes/caption.parquet")
    val SO_caption_likes = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SO/caption/likes.parquet")


    //L2 FOR 10M STRUCTURE
    val SS_nationality_likes = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/nationality/likes.parquet")
    val SS_likes_nationality = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/likes/nationality.parquet")
    val VP_parentCountry = spark.read.format("parquet").load(s"$path/VP/Parquet/parentCountry.parquet")

    //L3 FOR 10M STRUCTURE
    val SS_likes_subscribes = spark.read.format("parquet").load(s"$path/ExtVP/Parquet/SS/likes/subscribes.parquet")

    //C1
    SS_caption_hasReview.createOrReplaceTempView("SS_caption_hasReview")
    SS_contentRating_caption.createOrReplaceTempView("SS_contentRating_caption")
    SS_text_caption.createOrReplaceTempView("SS_text_caption")
    SS_hasReview_caption.createOrReplaceTempView("SS_hasReview_caption")
    VP_Rev_title.createOrReplaceTempView("VP_Rev_title")
    VP_Reviewer.createOrReplaceTempView("VP_Reviewer")
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


    //create file to write the query run time results
    //    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs2/$ds/orc/VP/$ds.txt"),true)

    val queries = List(new ExtVPQueries L3)
    /*  new VTQueries q10) ,
      new VTQueries q3,
      new VTQueries q4,
      new VTQueries q5,
      new VTQueries q6,
      new VTQueries q7,
      new VTQueries q8,
      new VTQueries q9,
      new VTQueries q10,
      new VTQueries q11) */


    var count = 1
    for (query <- queries) {
      //run query and calculate the run time
      val startTime = System.nanoTime()
      val df_count = spark.sql(query).count()
      println(df_count)
      //df.take(100).foreach(println)
      val endTime = System.nanoTime()
      val result = (endTime - startTime).toDouble / 1000000000

      //      //write the result into the log file
      //      if (count != queries.size) {
      //        Console.withOut(fos) {
      //          print(result + ",")
      //        }
      //      } else {
      //        Console.withOut(fos) {
      //          println(result)
      //        }
      //      }
      count += 1
    }
    println("All Queries are Done - Parquet - VP!")


  }
}
