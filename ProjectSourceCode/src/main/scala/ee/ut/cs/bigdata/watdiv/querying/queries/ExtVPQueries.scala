package ee.ut.cs.bigdata.watdiv.querying.queries

class ExtVPQueries {

  //Complex

  val c1 =
    """
      |SELECT tab0.v1 AS v1 , tab0.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2
      |FROM    (SELECT obj AS v1 , subject AS v0
      |FROM SS_caption_hasReview
      |) tab0
      |JOIN    (SELECT subject AS v0 , obj AS v3
      |FROM SS_contentRating_caption
      |) tab2
      |ON(tab0.v0=tab2.v0)
      |JOIN    (SELECT subject AS v0 , obj AS v2
      |FROM SS_text_caption
      |) tab1
      |ON(tab2.v0=tab1.v0)
      |JOIN    (SELECT subject AS v0 , obj AS v4
      |FROM SS_hasReview_caption
      |) tab3 ON(tab1.v0=tab3.v0)
      |JOIN    (SELECT obj AS v5 , subject AS v4 FROM SO_title_hasReview) tab4
      |ON(tab3.v4=tab4.v4)
      |JOIN    (SELECT obj AS v6 , subject AS v4
      |FROM SS_reviewer_title) tab5
      |ON(tab4.v4=tab5.v4)
      |JOIN    (SELECT subject AS v7 , obj AS v6
      |FROM SS_actor_language
      |) tab6
      |ON(tab5.v6=tab6.v6)
      |JOIN    (SELECT subject AS v7 , obj AS v8
      |FROM SS_language_actor
      |) tab7
      |ON(tab6.v7=tab7.v7)
      |""".stripMargin

  //This One Works for the 10M Dataset (Some SS_SO_OS are not there!!)
  val c1_copy =
    """
      |SELECT tab0.v1 AS v1 , tab0.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2
      |FROM    (SELECT object AS v1 , subject AS v0
      |FROM SS_caption_hasReview
      |) tab0
      |JOIN    (SELECT subject AS v0 , object AS v3
      |FROM SS_contentRating_caption
      |) tab2
      |ON(tab0.v0=tab2.v0)
      |JOIN    (SELECT subject AS v0 , object AS v2
      |FROM SS_text_caption
      |) tab1
      |ON(tab2.v0=tab1.v0)
      |JOIN    (SELECT subject AS v0 , object AS v4
      |FROM SS_hasReview_caption
      |) tab3 ON(tab1.v0=tab3.v0)
      |JOIN    (SELECT object AS v5 , subject AS v4 FROM VP_Rev_title) tab4
      |ON(tab3.v4=tab4.v4)
      |JOIN    (SELECT object AS v6 , subject AS v4
      |FROM VP_Reviewer) tab5
      |ON(tab4.v4=tab5.v4)
      |JOIN    (SELECT subject AS v7 , object AS v6
      |FROM SS_actor_language
      |) tab6
      |ON(tab5.v6=tab6.v6)
      |JOIN    (SELECT subject AS v7 , object AS v8
      |FROM SS_language_actor
      |) tab7
      |ON(tab6.v7=tab7.v7)
      |""".stripMargin


  val c2 =
    """
      |SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab4.v5 AS v5 , tab7.v7 AS v7 , tab5.v6 AS v6 , tab6.v4 AS v4 , tab9.v9 AS v9 , tab3.v3 AS v3 , tab8.v8 AS v8 , tab2.v2 AS v2
      |FROM    (SELECT subject AS v2
      |FROM SO_eligibleRegion_offers
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Country3') tab2
      |JOIN    (SELECT subject AS v0 , object AS v2
      |FROM SS_offers_legalName) tab1
 ON(tab2.v2=tab1.v2)
 JOIN    (SELECT object AS v1 , subject AS v0
  FROM VP_legalName

 ) tab0
 ON(tab1.v0=tab0.v0)
 JOIN    (SELECT object AS v3 , subject AS v2
  FROM OS_includes_hasReview
 ) tab3
 ON(tab1.v2=tab3.v2)
 JOIN    (SELECT subject AS v3 , object AS v8
  FROM OS_hasReview_totalVotes

 ) tab8
 ON(tab3.v3=tab8.v3)
 JOIN    (SELECT object AS v9 , subject AS v8
  FROM SO_totalVotes_hasReview

 ) tab9
 ON(tab8.v8=tab9.v8)
 JOIN    (SELECT subject AS v7 , object AS v3
  FROM OS_purchaseFor_hasReview

 ) tab7
 ON(tab8.v3=tab7.v3)
 JOIN    (SELECT object AS v7 , subject AS v4
  FROM SS_makesPurchase_homepage

 ) tab6
 ON(tab7.v7=tab6.v7)
 JOIN    (SELECT object AS v5 , subject AS v4
  FROM SS_jobTitle_homepage

 ) tab4
 ON(tab6.v4=tab4.v4)
 JOIN    (SELECT object AS v6 , subject AS v4
  FROM SS_homepage_jobTitle


 ) tab5
 ON(tab4.v4=tab5.v4)
      |""".stripMargin


  val c3 =
    """
      |SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2
      |FROM    (SELECT subject AS v0 , object AS v3
      |FROM SS_Location_likes) tab2
      |JOIN    (SELECT subject AS v0 , object AS v4
      |FROM SS_age_likes) tab3
      |ON(tab2.v0=tab3.v0)
      |JOIN    (SELECT subject AS v0 , object AS v5
      |FROM SS_gender_likes ) tab4
      |ON(tab3.v0=tab4.v0)
      |JOIN    (SELECT subject AS v0 , object AS v6
      |FROM SS_givenName_likes) tab5
      |ON(tab4.v0=tab5.v0)
      |JOIN    (SELECT object AS v1 , subject AS v0
      |FROM SS_likes_Location) tab0
      |ON(tab5.v0=tab0.v0)
      |JOIN    (SELECT subject AS v0 , object AS v2
      |FROM SS_friendOf_likes) tab1
      |ON(tab0.v0=tab1.v0)
      |""".stripMargin



  // Snow-Flake (F)


  val F1 =
    """
      |SELECT tab0.v0 AS v0 , tab3.v5 AS v5 , tab2.v4 AS v4 , tab4.v3 AS v3 , tab1.v2 AS v2
      |FROM    (SELECT subject AS v0
      |FROM SO_tag_hasGenre
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Topic47') tab0
      |JOIN  (SELECT subject AS v0 , object AS v2
      |FROM SO_type_hasGenre) tab1
      |ON(tab0.v0=tab1.v0)
      |JOIN    (SELECT object AS v0 , subject AS v3
      |FROM SS_hasGenre_trailer
      |) tab4
      |ON(tab1.v0=tab4.v0)
      |JOIN    (SELECT subject AS v3
      |FROM SS_type_trailer
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2') tab5
      |ON(tab4.v3=tab5.v3)
      |JOIN    (SELECT object AS v5 , subject AS v3
      |FROM SS_keywords_trailer) tab3
      |ON(tab5.v3=tab3.v3)
      |JOIN (SELECT object AS v4 , subject AS v3
      |FROM SS_trailer_keywords) tab2
      |ON(tab3.v3=tab2.v3)
      |""".stripMargin


  val F2 =
    """
      |SELECT tab0.v1 AS v1 , tab7.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2
 FROM    (SELECT subject AS v0
	 FROM SS_hasGenre_caption

	 WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre62'
	) tab7
 JOIN    (SELECT object AS v1 , subject AS v0
	 FROM SS_homepage_caption

	) tab0
 ON(tab7.v0=tab0.v0)
 JOIN    (SELECT subject AS v0 , object AS v4
	 FROM SS_caption_homepage

	) tab3
 ON(tab0.v0=tab3.v0)
 JOIN    (SELECT subject AS v0 , object AS v5
	 FROM SS_description_caption

	) tab4
 ON(tab3.v0=tab4.v0)
 JOIN    (SELECT subject AS v0 , object AS v2
	 FROM SS_title_caption
	) tab1
 ON(tab4.v0=tab1.v0)
 JOIN    (SELECT subject AS v0 , object AS v3
	 FROM SS_type_caption
	) tab2
 ON(tab1.v0=tab2.v0)
 JOIN    (SELECT subject AS v1 , object AS v6
	 FROM SO_url_homepage
	) tab5
 ON(tab0.v1=tab5.v1)
 JOIN    (SELECT subject AS v1 , object AS v7
	 FROM SO_hits_homepage
	) tab6
 ON(tab5.v1=tab6.v1)
      |""".stripMargin


  val F3 =
    """
      |SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab5.v5 AS v5 , tab4.v6 AS v6 , tab3.v4 AS v4 , tab1.v2 AS v2
 FROM    (SELECT subject AS v0
	 FROM SS_hasGenre_contentSize

	 WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre111'
	) tab2
 JOIN    (SELECT object AS v1 , subject AS v0
	 FROM SS_contentRating_contentSize

	) tab0
 ON(tab2.v0=tab0.v0)
 JOIN    (SELECT subject AS v0 , object AS v2
	 FROM SS_contentSize_contentRating

	) tab1
 ON(tab0.v0=tab1.v0)
 JOIN    (SELECT object AS v0 , subject AS v5
	 FROM OS_purchaseFor_contentSize

	) tab5
 ON(tab1.v0=tab5.v0)
 JOIN    (SELECT object AS v5 , subject AS v4
	 FROM OS_makesPurchase_purchaseDate

	) tab3
 ON(tab5.v5=tab3.v5)
 JOIN    (SELECT subject AS v5 , object AS v6
	 FROM SO_purchaseDate_makesPurchase

	) tab4
 ON(tab3.v5=tab4.v5)

      |""".stripMargin


  val F3_10M =
    """
      |SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab5.v5 AS v5 , tab4.v6 AS v6 , tab3.v4 AS v4 , tab1.v2 AS v2
 FROM    (SELECT subject AS v0
	 FROM SS_hasGenre_contentSize

	 WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre131'
	) tab2
 JOIN    (SELECT object AS v1 , subject AS v0
	 FROM SS_contentRating_contentSize

	) tab0
 ON(tab2.v0=tab0.v0)
 JOIN    (SELECT subject AS v0 , object AS v2
	 FROM SS_contentSize_contentRating

	) tab1
 ON(tab0.v0=tab1.v0)
 JOIN    (SELECT object AS v0 , subject AS v5
	 FROM OS_purchaseFor_contentSize

	) tab5
 ON(tab1.v0=tab5.v0)
 JOIN    (SELECT object AS v5 , subject AS v4
	 FROM OS_makesPurchase_purchaseDate

	) tab3
 ON(tab5.v5=tab3.v5)
 JOIN    (SELECT subject AS v5 , object AS v6
	 FROM VP_purchaseDate

	) tab4
 ON(tab3.v5=tab4.v5)

      |""".stripMargin


  val F4 =
    """
      |SELECT tab7.v1 AS v1 , tab0.v0 AS v0 , tab8.v7 AS v7 , tab5.v5 AS v5 , tab6.v6 AS v6 , tab3.v4 AS v4 , tab1.v2 AS v2 , tab4.v8 AS v8
     FROM    (SELECT subject AS v1
	 FROM SO_language_homepage

	 WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Language0'
	) tab7
 JOIN    (SELECT object AS v1 , subject AS v0
	 FROM SS_homepage_contentSize

	) tab0
 ON(tab7.v1=tab0.v1)
 JOIN    (SELECT subject AS v0
	 FROM SS_tag_contentSize
	 WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Topic52'
	) tab2
 ON(tab0.v0=tab2.v0)
 JOIN    (SELECT subject AS v0 , object AS v8
	 FROM SS_contentSize_homepage

	) tab4
 ON(tab2.v0=tab4.v0)
 JOIN    (SELECT subject AS v0 , object AS v4
	 FROM SS_description_contentSize

	) tab3
 ON(tab4.v0=tab3.v0)
 JOIN    (SELECT subject AS v1 , object AS v5
	 FROM SO_url_homepage
	) tab5
 ON(tab0.v1=tab5.v1)
 JOIN    (SELECT subject AS v1 , object AS v6
	 FROM SO_hits_homepage
	) tab6
 ON(tab5.v1=tab6.v1)
 JOIN    (SELECT object AS v0 , subject AS v2
	 FROM OS_includes_contentSize
	) tab1
 ON(tab3.v0=tab1.v0)
 JOIN    (SELECT object AS v0 , subject AS v7
	 FROM OS_likes_contentSize
	) tab8
 ON(tab1.v0=tab8.v0)
 |""".stripMargin


  val F5 =
    """
      |SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3
 FROM    (SELECT object AS v0
	 FROM OS_offers_validThrough
	 WHERE subject = 'http://db.uwaterloo.ca/~galuc/wsdbm/Retailer10'
	) tab1
 JOIN    (SELECT subject AS v0 , object AS v4
	 FROM SO_validThrough_offers

	) tab3
 ON(tab1.v0=tab3.v0)
 JOIN    (SELECT object AS v1 , subject AS v0
	 FROM SS_includes_validThrough
	) tab0
 ON(tab3.v0=tab0.v0)
 JOIN    (SELECT subject AS v1 , object AS v5
	 FROM SO_title_includes
	) tab4
 ON(tab0.v1=tab4.v1)
 JOIN    (SELECT subject AS v1 , object AS v6
	 FROM SO_type_includes
	) tab5
 ON(tab4.v1=tab5.v1)
 JOIN    (SELECT subject AS v0 , object AS v3
	 FROM SS_price_validThrough
	) tab2
 ON(tab0.v0=tab2.v0)
 |""".stripMargin


  // Linear (L)


  val L1 =
    """
      |SELECT tab0.v0 AS v0 , tab1.v3 AS v3 , tab2.v2 AS v2
      |FROM    (SELECT subject AS v0
      |FROM SS_subscribes_likes
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Website30') tab0
      |JOIN    (SELECT subject AS v0 , object AS v2
      |FROM OS_likes_caption) tab2
      |ON(tab0.v0=tab2.v0)
      |JOIN    (SELECT object AS v3 , subject AS v2
      |FROM SO_caption_likes) tab1
      |ON(tab2.v2=tab1.v2)
      |""".stripMargin


  val L2 =
    """
      |SELECT tab0.v1 AS v1 , tab2.v2 AS v2
      |FROM    (SELECT object AS v1
      |FROM VP_parentCountry
      |WHERE subject = 'http://db.uwaterloo.ca/~galuc/wsdbm/City152' ) tab0
      |JOIN    (SELECT object AS v1 , subject AS v2
      |FROM SS_nationality_likes) tab2
      |ON(tab0.v1=tab2.v1)
      |JOIN    (SELECT subject AS v2 FROM SS_likes_nationality
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Product0') tab1
      |ON(tab2.v2=tab1.v2)
      |""".stripMargin


  val L3 =
    """
      |SELECT tab1.v1 AS v1 , tab0.v0 AS v0
      |FROM    (SELECT subject AS v0
      |FROM SS_subscribes_likes
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Website34') tab0
      |JOIN    (SELECT object AS v1 , subject AS v0
      |FROM SS_likes_subscribes) tab1
      |ON(tab0.v0=tab1.v0)
      |""".stripMargin


  val L4 =
    """
      |SELECT tab0.v0 AS v0 , tab1.v2 AS v2
      |FROM    (SELECT subject AS v0
      |FROM SS_tag_caption WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Topic248') tab0
      |JOIN    (SELECT subject AS v0 , object AS v2
      |FROM SS_caption_tag) tab1
      |ON(tab0.v0=tab1.v0)
      |""".stripMargin


  val L5 =
    """
    |SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab1.v3 AS v3
    |FROM    (SELECT object AS v3
    |FROM VP_parentCountry

	 WHERE subject = 'http://db.uwaterloo.ca/~galuc/wsdbm/City187'
	) tab1
 JOIN    (SELECT subject AS v0 , object AS v3
	 FROM SS_nationality_jobTitle

	) tab2
 ON(tab1.v3=tab2.v3)
 JOIN    (SELECT object AS v1 , subject AS v0
	 FROM SS_jobTitle_nationality

	) tab0
 ON(tab2.v0=tab0.v0)

      |""".stripMargin


  //Star (S)


  val S1 =
    """
      |SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab3.v4 AS v4 , tab5.v6 AS v6 , tab2.v3 AS v3 , tab8.v9 AS v9 , tab7.v8 AS v8
      |FROM    (SELECT object AS v0
      |FROM OS_offers_priceValidUntil
      |WHERE subject = 'http://db.uwaterloo.ca/~galuc/wsdbm/Retailer4') tab1
      |JOIN    (SELECT subject AS v0 , object AS v5
      |FROM SS_validFrom_priceValidUntil) tab4
      |ON(tab1.v0=tab4.v0)
      |JOIN    (SELECT subject AS v0 , object AS v9
      |FROM SS_priceValidUntil_validFrom) tab8
      |ON(tab4.v0=tab8.v0)
      |JOIN    (SELECT subject AS v0 , object AS v6
      |FROM SS_validThrough_priceValidUntil) tab5
      |ON(tab8.v0=tab5.v0)
      |JOIN    (SELECT object AS v1 , subject AS v0
      |FROM SS_includes_priceValidUntil) tab0
      |ON(tab5.v0=tab0.v0)
      |JOIN    (SELECT subject AS v0 , object AS v3
      |FROM SS_price_priceValidUntil) tab2
      |ON(tab0.v0=tab2.v0)
      |JOIN    (SELECT subject AS v0 , object AS v4
      |FROM SS_serialNumber_priceValidUntil) tab3
      |ON(tab2.v0=tab3.v0)
      |JOIN    (SELECT subject AS v0 , object AS v7
      |FROM SS_eligibleQuantity_priceValidUntil) tab6
      |ON(tab3.v0=tab6.v0)
      |JOIN    (SELECT subject AS v0 , object AS v8
      |FROM SS_eligibleRegion_priceValidUntil) tab7
      |ON(tab6.v0=tab7.v0)
      |""".stripMargin


  val S2 =
    """
      |SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab2.v3 AS v3
      |FROM    (SELECT subject AS v0
      |FROM SS_nationality_Location
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Country3') tab1
      |JOIN    (SELECT subject AS v0
      |FROM SS_type_nationality
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Role2') tab3
      |ON(tab1.v0=tab3.v0)
      |JOIN    (SELECT object AS v1 , subject AS v0
      |FROM SS_Location_nationality) tab0
      |ON(tab3.v0=tab0.v0)
      |JOIN    (SELECT subject AS v0 , object AS v3
      |FROM SS_gender_nationality) tab2
      |ON(tab0.v0=tab2.v0)
      |""".stripMargin


  val S3 =
    """
      |SELECT tab0.v0 AS v0 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2
      |FROM    (SELECT subject AS v0
      |FROM SS_type_publisher
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory4') tab0
      |JOIN    (SELECT subject AS v0 , object AS v2
      |FROM SS_caption_publisher) tab1
      |ON(tab0.v0=tab1.v0)
      |JOIN    (SELECT subject AS v0 , object AS v4
      |FROM SS_publisher_caption) tab3
      |ON(tab1.v0=tab3.v0)
      |JOIN    (SELECT subject AS v0 , object AS v3
      |FROM SS_hasGenre_publisher) tab2
      |ON(tab3.v0=tab2.v0)
      |""".stripMargin


  val S4 =
    """
      |SELECT tab3.v0 AS v0 , tab2.v3 AS v3 , tab1.v2 AS v2
      |FROM    (SELECT subject AS v0
      |FROM SO_nationality_artist
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Country1') tab3
      |JOIN    (SELECT subject AS v0
      |FROM SO_age_artist
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup1') tab0
      |ON(tab3.v0=tab0.v0)
      |JOIN    (SELECT object AS v0 , subject AS v3
      |FROM OS_artist_nationality) tab2
      |ON(tab0.v0=tab2.v0)
      |JOIN    (SELECT subject AS v0 , object AS v2
      |FROM SO_familyName_artist) tab1
      |ON(tab2.v0=tab1.v0)
      |""".stripMargin


  val S5 =
    """
      |SELECT tab3.v0 AS v0 , tab2.v3 AS v3 , tab1.v2 AS v2
      |FROM    (SELECT subject AS v0
      |FROM SS_language_keywords
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Language0') tab3
      |JOIN    (SELECT subject AS v0
      |FROM SS_type_language
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2') tab0
      |ON(tab3.v0=tab0.v0)
      |JOIN    (SELECT subject AS v0 , object AS v3
      |FROM SS_keywords_language) tab2
      |ON(tab0.v0=tab2.v0)
      |JOIN    (SELECT subject AS v0 , object AS v2
      |FROM SS_description_language) tab1
      |ON(tab2.v0=tab1.v0)
      |""".stripMargin


  val S6 =
    """
      |SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab1.v2 AS v2
      |FROM    (SELECT subject AS v0
      |FROM SS_hasGenre_conductor
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre115') tab2
      |JOIN    (SELECT object AS v1 , subject AS v0
      |FROM VP_conductor) tab0
      |ON(tab2.v0=tab0.v0)
      |JOIN    (SELECT subject AS v0 , object AS v2
      |FROM SS_type_conductor) tab1
      |ON(tab0.v0=tab1.v0)
      |""".stripMargin


  val S7 =
    """
      |SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab1.v2 AS v2
      |FROM    (SELECT object AS v0
      |FROM OS_likes_text
      |WHERE subject = 'http://db.uwaterloo.ca/~galuc/wsdbm/User100') tab2
      |JOIN    (SELECT subject AS v0 , object AS v2
      |FROM SO_text_likes) tab1
      |ON(tab2.v0=tab1.v0)
      |JOIN    (SELECT object AS v1 , subject AS v0
      |FROM SS_type_text) tab0
      |ON(tab1.v0=tab0.v0)
      |""".stripMargin
}