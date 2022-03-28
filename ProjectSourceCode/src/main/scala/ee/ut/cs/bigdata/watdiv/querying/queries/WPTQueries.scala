package ee.ut.cs.bigdata.watdiv.querying.queries

class WPTQueries {

///*********** COMPLEX ************////

val c1_prost =
    """
    |SELECT DISTINCT V0.s, hasReview, V4.http___purl_org_stuff_rev_reviewer, language
    |FROM WPT V0
    |JOIN WPT V4
    |ON array_contains (V0.http___purl_org_stuff_rev_hasReview,V4.s)
    |JOIN WPT V6
    |ON V4.http___purl_org_stuff_rev_reviewer=V6.s
    |JOIN WPT V7
    |ON  array_contains(V7.http___schema_org_actor, V6.s)
    |lateral view outer explode (V0.http___purl_org_stuff_rev_hasReview) V0HasReview as hasReview
    |lateral view outer explode (V7.http___schema_org_language) V7Langauage as language

    |WHERE  V0.http___schema_org_caption is not null
    |AND V0.http___schema_org_text is not null
    |AND V0.http___schema_org_contentRating is not null
    |AND V4.http___purl_org_stuff_rev_title is not null
    |AND V7.http___schema_org_language is not null
  """.stripMargin


val c1_prost_csv =
    """
    |SELECT DISTINCT V0.s, hasReview, V4.http___purl_org_stuff_rev_reviewer, V7.s
    |FROM WPT V0
    |JOIN WPT V4
    |ON array_contains (V0.http___purl_org_stuff_rev_hasReview,V4.s)
    |JOIN WPT V6
    |ON V4.http___purl_org_stuff_rev_reviewer=V6.s
    |JOIN WPT V7
    |ON  array_contains(V7.http___schema_org_actor, V6.s)
    |lateral view explode (V0.http___purl_org_stuff_rev_hasReview) V0HasReview as hasReview
    |WHERE  V0.http___schema_org_caption is not null
    |AND V0.http___schema_org_text is not null
    |AND V0.http___schema_org_contentRating is not null
    |AND V4.http___purl_org_stuff_rev_title is not null
    |AND V7.http___schema_org_language is not null
  """.stripMargin




  val c2_prost =
    """
      |SELECT DISTINCT V0.s, V3.s, V4.s, V8.s
      |FROM WPT V0
      |JOIN WPT V2
      |ON  array_contains(V0.http___purl_org_goodrelations_offers,V2.s)
      |AND array_contains(V2.http___schema_org_eligibleRegion,"<http://db.uwaterloo.ca/~galuc/wsdbm/Country3>")
      |JOIN WPT V3 ON V2.http___purl_org_goodrelations_includes=V3.s
      |JOIN WPT V7 ON V7.http___db_uwaterloo_ca__galuc_wsdbm_purchaseFor=V3.s
      |JOIN WPT V4 ON array_contains (V4.http___db_uwaterloo_ca__galuc_wsdbm_makesPurchase,V7.s)
      |JOIN WPT V8 ON array_contains(V3.http___purl_org_stuff_rev_hasReview,V8.s)
      |WHERE V4.http___schema_org_jobTitle is not null
      |AND V0.http___schema_org_legalName is not null
      |AND V4.http___xmlns_com_foaf_homepage is not null
      |AND V8.http___purl_org_stuff_rev_totalVotes is not null
      |""".stripMargin


//Fails because of driver-memory

  val c2_prost_csv =
    """
      |SELECT DISTINCT V0.s, V3.s, V4.s, V8.s
      |FROM WPT V0
      |JOIN WPT V2
      |ON  array_contains(split(rtrim(']', ltrim('[',V0.http___purl_org_goodrelations_offers)),','),V2.s)
      |AND array_contains(split(rtrim(']', ltrim('[',V2.http___schema_org_eligibleRegion)),',') ,"<http://db.uwaterloo.ca/~galuc/wsdbm/Country3>")
      |JOIN WPT V3 ON V2.http___purl_org_goodrelations_includes=V3.s
      |JOIN WPT V7 ON V7.http___db_uwaterloo_ca__galuc_wsdbm_purchaseFor=V3.s
      |JOIN WPT V4 ON array_contains (split(rtrim(']', ltrim('[',V4.http___db_uwaterloo_ca__galuc_wsdbm_makesPurchase)),','),V7.s)
      |JOIN WPT V8 ON array_contains( split(rtrim(']', ltrim('[',V3.http___purl_org_stuff_rev_hasReview)),','),V8.s)
      |WHERE V4.http___schema_org_jobTitle is not null
      |AND V0.http___schema_org_legalName is not null
      |AND V4.http___xmlns_com_foaf_homepage is not null
      |AND V8.http___purl_org_stuff_rev_totalVotes is not null
      |""".stripMargin


val c3_prost =
    """
      |SELECT  T0.s
      |FROM  WPT T0
      |
      |WHERE T0.http___db_uwaterloo_ca__galuc_wsdbm_likes  is not null
      |AND   T0.http___db_uwaterloo_ca__galuc_wsdbm_friendOf  is not null
      |AND   T0.http___purl_org_dc_terms_Location  is not null
      |AND   T0.http___xmlns_com_foaf_age       is not null
      |AND   T0.http___db_uwaterloo_ca__galuc_wsdbm_gender    is not null
      |AND   T0.http___xmlns_com_foaf_givenName is not null
      |""".stripMargin


// C3 with the CSV is the same


val c3_prost_csv =
    """
      |SELECT  T0.s
      |FROM  WPT T0
      |
      |WHERE T0.http___db_uwaterloo_ca__galuc_wsdbm_likes  is not null
      |AND   T0.http___db_uwaterloo_ca__galuc_wsdbm_friendOf  is not null
      |AND   T0.http___purl_org_dc_terms_Location  is not null
      |AND   T0.http___xmlns_com_foaf_age       is not null
      |AND   T0.http___db_uwaterloo_ca__galuc_wsdbm_gender    is not null
      |AND   T0.http___xmlns_com_foaf_givenName is not null
      |""".stripMargin


  ///*********** SNOWFLAKES ************////


val f1_prost =
    """
      |SELECT DISTINCT V0.s, V0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type, V3.s, trailer, V3.http___schema_org_keywords
      |FROM WPT V0
      |JOIn WPT V3
      |ON   array_contains   (V3.http___db_uwaterloo_ca__galuc_wsdbm_hasGenre,V0.s)
      |AND  array_contains  (V0.http___ogp_me_ns_tag, "<http://db.uwaterloo.ca/~galuc/wsdbm/Topic8>")
      |lateral view explode (V3.http___schema_org_trailer) V3Trailer as trailer
      |WHERE array_contains(V3.http___www_w3_org_1999_02_22_rdf_syntax_ns_type, "<http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2>")
      |AND V3.http___schema_org_keywords is not null
      |AND V3.http___schema_org_trailer is not null
      |""".stripMargin


val f1_prost_csv =
    """
      |SELECT DISTINCT V0.s, V0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type, V3.s, trailer, V3.http___schema_org_keywords
      |FROM WPT V0
      |JOIn WPT V3
      |ON   array_contains   (split(rtrim(']', ltrim('[',V3.http___db_uwaterloo_ca__galuc_wsdbm_hasGenre)),',') ,V0.s)
      |AND  array_contains   (split(rtrim(']', ltrim('[',V0.http___ogp_me_ns_tag)),',') , "<http://db.uwaterloo.ca/~galuc/wsdbm/Topic8>")
      |lateral view explode (split(rtrim(']', ltrim('[',V3.http___schema_org_trailer)),',') ) V3Trailer as trailer
      |WHERE array_contains(split(rtrim(']', ltrim('[',V3.http___www_w3_org_1999_02_22_rdf_syntax_ns_type)),',')   , "<http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2>")
      |AND V3.http___schema_org_keywords is not null
      |AND V3.http___schema_org_trailer is not null
      |""".stripMargin


  val f2_prost =
    """
      |SELECT DISTINCT V0.s, V0.http___xmlns_com_foaf_homepage, V0.http___ogp_me_ns_title, V0.http___schema_org_caption, V0.http___schema_org_description, V1.http___schema_org_url, V1.http___db_uwaterloo_ca__galuc_wsdbm_hits
      |FROM WPT V0
      |JOIN WPT V1
      |ON V0.http___xmlns_com_foaf_homepage =V1.s
      |WHERE array_contains (V0.http___db_uwaterloo_ca__galuc_wsdbm_hasGenre,"<http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre117>")
      |AND V0.http___ogp_me_ns_title is not null
      |AND V0.http___schema_org_caption is not null
      |AND V0.http___schema_org_description is not null
      |AND V1.http___schema_org_url is not null
      |AND V1.http___db_uwaterloo_ca__galuc_wsdbm_hits is not null
      |""".stripMargin



  val f2_prost_csv =
    """
      |SELECT DISTINCT V0.s, V0.http___xmlns_com_foaf_homepage, V0.http___ogp_me_ns_title, V0.http___schema_org_caption, V0.http___schema_org_description, V1.http___schema_org_url, V1.http___db_uwaterloo_ca__galuc_wsdbm_hits
      |FROM WPT V0
      |JOIN WPT V1
      |ON V0.http___xmlns_com_foaf_homepage =V1.s
      |WHERE array_contains (split(rtrim(']', ltrim('[',V0.http___db_uwaterloo_ca__galuc_wsdbm_hasGenre)),',') ,"<http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre117>")
      |AND V0.http___ogp_me_ns_title is not null
      |AND V0.http___schema_org_caption is not null
      |AND V0.http___schema_org_description is not null
      |AND V1.http___schema_org_url is not null
      |AND V1.http___db_uwaterloo_ca__galuc_wsdbm_hits is not null
      |""".stripMargin


val f3_prost =

    """
      |SELECT DISTINCT V0.s, V0.http___schema_org_contentRating, V0.http___schema_org_contentSize, V4.s, V5.s, V5.http___db_uwaterloo_ca__galuc_wsdbm_purchaseDate
      |FROM WPT V0
      |JOIN WPT V5
      |ON V0.s=V5.http___db_uwaterloo_ca__galuc_wsdbm_purchaseFor
      |AND array_contains (V0.http___db_uwaterloo_ca__galuc_wsdbm_hasGenre,"<http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre111>")
      |JOIN WPT V4
      |ON  array_contains (V4.http___db_uwaterloo_ca__galuc_wsdbm_makesPurchase,V5.s)
      |WHERE V0.http___schema_org_contentRating is not null
      |AND V0.http___schema_org_contentSize is not null
      |AND V5.http___db_uwaterloo_ca__galuc_wsdbm_purchaseDate is not null
      |""".stripMargin



val f3_prost_csv =

    """
      |SELECT DISTINCT V0.s, V0.http___schema_org_contentRating, V0.http___schema_org_contentSize, V4.s, V5.s, V5.http___db_uwaterloo_ca__galuc_wsdbm_purchaseDate
      |FROM WPT V0
      |JOIN WPT V5
      |ON V0.s=V5.http___db_uwaterloo_ca__galuc_wsdbm_purchaseFor
      |AND array_contains (split(rtrim(']', ltrim('[',V0.http___db_uwaterloo_ca__galuc_wsdbm_hasGenre)),','),"<http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre111>")
      |JOIN WPT V4
      |ON  array_contains ( split(rtrim(']', ltrim('[',V4.http___db_uwaterloo_ca__galuc_wsdbm_makesPurchase)),',')  ,V5.s)
      |WHERE V0.http___schema_org_contentRating is not null
      |AND V0.http___schema_org_contentSize is not null
      |AND V5.http___db_uwaterloo_ca__galuc_wsdbm_purchaseDate is not null
      |""".stripMargin



  val f4_prost =

    """
      |SELECT DISTINCT V0.s, V1.s, V2.s, V0.http___schema_org_description, V0.http___schema_org_contentSize, V1.http___schema_org_url, V1.http___db_uwaterloo_ca__galuc_wsdbm_hits, V7.s
      |FROM WPT V0
      |JOIN WPT V1
      |ON V0.http___xmlns_com_foaf_homepage=V1.s
      |AND   array_contains (V1.http___schema_org_language,"<http://db.uwaterloo.ca/~galuc/wsdbm/Language0>")
      |JOIN WPT V2
      |ON V0.s=V2.http___purl_org_goodrelations_includes
      |JOIN WPT V7
      |ON   array_contains (V7.http___db_uwaterloo_ca__galuc_wsdbm_likes,V0.s)
      |WHERE array_contains (V0.http___ogp_me_ns_tag,"<http://db.uwaterloo.ca/~galuc/wsdbm/Topic122>")
      |AND V0.http___schema_org_description is not null
      |AND V0.http___schema_org_contentSize is not null
      |AND V1.http___schema_org_url is not null
      |AND V1.http___db_uwaterloo_ca__galuc_wsdbm_hits is not null
      |""".stripMargin


  val f4_prost_csv =

    """
      |SELECT DISTINCT V0.s, V1.s, V2.s, V0.http___schema_org_description, V0.http___schema_org_contentSize, V1.http___schema_org_url, V1.http___db_uwaterloo_ca__galuc_wsdbm_hits, V7.s
      |FROM WPT V0
      |JOIN WPT V1
      |ON V0.http___xmlns_com_foaf_homepage=V1.s
      |AND   array_contains (split(rtrim(']', ltrim('[',V1.http___schema_org_language)),','),"<http://db.uwaterloo.ca/~galuc/wsdbm/Language0>")
      |JOIN WPT V2
      |ON V0.s=V2.http___purl_org_goodrelations_includes
      |JOIN WPT V7
      |ON   array_contains (split(rtrim(']', ltrim('[',V7.http___db_uwaterloo_ca__galuc_wsdbm_likes)),',') ,V0.s)
      |WHERE array_contains (split(rtrim(']', ltrim('[',V0.http___ogp_me_ns_tag)),',') ,"<http://db.uwaterloo.ca/~galuc/wsdbm/Topic122>")
      |AND V0.http___schema_org_description is not null
      |AND V0.http___schema_org_contentSize is not null
      |AND V1.http___schema_org_url is not null
      |AND V1.http___db_uwaterloo_ca__galuc_wsdbm_hits is not null
      |""".stripMargin


  val f5_prost =
    """
      |SELECT DISTINCT V0.s, V1.s, V0.http___purl_org_goodrelations_price, V0.http___purl_org_goodrelations_validThrough, V1.http___ogp_me_ns_title, type
      |FROM WPT V0
      |JOIN WPT V1
      |ON V0.http___purl_org_goodrelations_includes=V1.s
      |JOIN WPT V2
      |ON  array_contains(V2.http___purl_org_goodrelations_offers,V0.s)
      |AND V2.s="<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer9885>"
      |lateral view explode (V1.http___www_w3_org_1999_02_22_rdf_syntax_ns_type) V1Type as type
      |WHERE V0.http___purl_org_goodrelations_price is not null
      |AND V0.http___purl_org_goodrelations_validThrough is not null
      |AND V1.http___ogp_me_ns_title is not null
      |AND V1.http___www_w3_org_1999_02_22_rdf_syntax_ns_type is not null
      |""".stripMargin


  val f5_prost_csv =
    """
      |SELECT DISTINCT V0.s, V1.s, V0.http___purl_org_goodrelations_price,
      |V0.http___purl_org_goodrelations_validThrough, V1.http___ogp_me_ns_title, type
      |FROM WPT V0
      |JOIN WPT V1
      |ON V0.http___purl_org_goodrelations_includes=V1.s
      |JOIN WPT V2
      |ON  array_contains(split(rtrim(']', ltrim('[',V2.http___purl_org_goodrelations_offers)),',') ,V0.s)
      |AND V2.s="<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer9885>"
      |lateral view explode (split(rtrim(']', ltrim('[',V1.http___www_w3_org_1999_02_22_rdf_syntax_ns_type)),',') ) V1Type as type
      |WHERE V0.http___purl_org_goodrelations_price is not null
      |AND V0.http___purl_org_goodrelations_validThrough is not null
      |AND V1.http___ogp_me_ns_title is not null
      |AND V1.http___www_w3_org_1999_02_22_rdf_syntax_ns_type is not null
      |""".stripMargin




///*********** LINEARS ************////

  val l1_prost =
    """
      |SELECT DISTINCT T0.s, T1.s, T1.http___schema_org_caption
      |FROM WPT T0
      |JOIN WPT T1
      |ON array_contains (T0.http___db_uwaterloo_ca__galuc_wsdbm_likes,T1.s)
      |WHERE  array_contains ( T0.http___db_uwaterloo_ca__galuc_wsdbm_subscribes ,"<http://db.uwaterloo.ca/~galuc/wsdbm/Website7355>")
      |AND T1.http___schema_org_caption  is not null
      |""".stripMargin


  val l1_prost_csv =
    """
      |SELECT DISTINCT T0.s, T1.s, T1.http___schema_org_caption
      |FROM WPT T0
      |JOIN WPT T1
      |ON array_contains (split(rtrim(']', ltrim('[',T0.http___db_uwaterloo_ca__galuc_wsdbm_likes)),',') ,T1.s)
      |WHERE  array_contains (split(rtrim(']', ltrim('[',T0.http___db_uwaterloo_ca__galuc_wsdbm_subscribes)),','),"<http://db.uwaterloo.ca/~galuc/wsdbm/Website7355>")
      |AND T1.http___schema_org_caption  is not null
      |""".stripMargin


  val l2_prost =
       """
      |SELECT DISTINCT T_User.s, TT.http___www_geonames_org_ontology_parentCountry
      |FROM WPT T_User
      |JOIN (
      |SELECT T_City.http___www_geonames_org_ontology_parentCountry
      |FROM WPT T_City
      |WHERE T_City.s="<http://db.uwaterloo.ca/~galuc/wsdbm/City70>") TT
      |ON TT.http___www_geonames_org_ontology_parentCountry=T_User.http___schema_org_nationality
      |WHERE array_contains(T_User.http___db_uwaterloo_ca__galuc_wsdbm_likes,"<http://db.uwaterloo.ca/~galuc/wsdbm/Product0>")
      |""".stripMargin



  val l2_prost_csv =
       """
      |SELECT DISTINCT T_User.s, TT.http___www_geonames_org_ontology_parentCountry
      |FROM WPT T_User
      |JOIN (
      |SELECT T_City.http___www_geonames_org_ontology_parentCountry
      |FROM WPT T_City
      |WHERE T_City.s="<http://db.uwaterloo.ca/~galuc/wsdbm/City70>") TT
      |ON TT.http___www_geonames_org_ontology_parentCountry=T_User.http___schema_org_nationality
      |WHERE array_contains(split(rtrim(']', ltrim('[',T_User.http___db_uwaterloo_ca__galuc_wsdbm_likes)),',')   ,"<http://db.uwaterloo.ca/~galuc/wsdbm/Product0>")
      |""".stripMargin


  val l3_prost =
    """
      |SELECT DISTINCT T0.s, likes
      |FROM WPT T0
      |lateral view explode (T0.http___db_uwaterloo_ca__galuc_wsdbm_likes) T0Likes as likes
      |WHERE array_contains (T0.http___db_uwaterloo_ca__galuc_wsdbm_subscribes ,"<http://db.uwaterloo.ca/~galuc/wsdbm/Website43164>")
      |AND T0.http___db_uwaterloo_ca__galuc_wsdbm_likes is not null
      |""".stripMargin

  val l3_prost_csv =
    """
      |SELECT DISTINCT T0.s, likes
      |FROM WPT T0
      |lateral view explode (split(rtrim(']', ltrim('[',T0.http___db_uwaterloo_ca__galuc_wsdbm_likes)),',')) T0Likes as likes
      |WHERE array_contains (split(rtrim(']', ltrim('[',T0.http___db_uwaterloo_ca__galuc_wsdbm_subscribes)),',')  ,"<http://db.uwaterloo.ca/~galuc/wsdbm/Website43164>")
      |AND T0.http___db_uwaterloo_ca__galuc_wsdbm_likes is not null
      |""".stripMargin

  val l4_prost =
    """
      |SELECT  DISTINCT T0.s, T0.http___schema_org_caption
      |FROM WPT T0
      |WHERE array_contains (T0.http___ogp_me_ns_tag, "<http://db.uwaterloo.ca/~galuc/wsdbm/Topic142>")
      |AND T0.http___schema_org_caption is not null
      |""".stripMargin

  val l4_prost_csv =
    """
      |SELECT  DISTINCT T0.s, T0.http___schema_org_caption
      |FROM WPT T0
      |WHERE array_contains (split(rtrim(']', ltrim('[',T0.http___ogp_me_ns_tag)),','), "<http://db.uwaterloo.ca/~galuc/wsdbm/Topic142>")
      |AND T0.http___schema_org_caption is not null
      |""".stripMargin


    val l5_prost =
       """
      |SELECT DISTINCT T_User.s, T_User.http___schema_org_jobTitle, TT.http___www_geonames_org_ontology_parentCountry
      |FROM WPT T_User
      |JOIN (
      |SELECT T_City.http___www_geonames_org_ontology_parentCountry
      |FROM WPT T_City
      |WHERE T_City.s="<http://db.uwaterloo.ca/~galuc/wsdbm/City40>") TT
      |ON TT.http___www_geonames_org_ontology_parentCountry=T_User.http___schema_org_nationality
      |WHERE T_User.http___schema_org_jobTitle is not null
      |""".stripMargin


  //The same query no difference
    val l5_prost_csv =
       """
      |SELECT DISTINCT T_User.s, T_User.http___schema_org_jobTitle, TT.http___www_geonames_org_ontology_parentCountry
      |FROM WPT T_User
      |JOIN (
      |SELECT T_City.http___www_geonames_org_ontology_parentCountry
      |FROM WPT T_City
      |WHERE T_City.s="<http://db.uwaterloo.ca/~galuc/wsdbm/City40>") TT
      |ON TT.http___www_geonames_org_ontology_parentCountry=T_User.http___schema_org_nationality
      |WHERE T_User.http___schema_org_jobTitle is not null
      |""".stripMargin


///*********** STARS ************////


val s1_prost =
"""
  |SELECT S1.s, S1.http___purl_org_goodrelations_includes, S1.http___purl_org_goodrelations_price,
  |S1.http___purl_org_goodrelations_serialNumber, S1.http___purl_org_goodrelations_validFrom, S1.http___purl_org_goodrelations_validThrough,
  |eligbileRegion, S1.http___schema_org_eligibleQuantity, S1.http___schema_org_priceValidUntil
  |FROM WPT S0
  |JOIN WPT S1
  |ON  array_contains(S0.http___purl_org_goodrelations_offers,S1.s)
  |AND S0.s='<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer8535>'
  |lateral view explode (S1.http___schema_org_eligibleRegion) S1Eligible as eligbileRegion
  |WHERE
  |S1.http___purl_org_goodrelations_includes  is not null
  |AND S1.http___purl_org_goodrelations_price is not null
  |AND S1.http___purl_org_goodrelations_serialNumber is not null
  |AND S1.http___purl_org_goodrelations_validFrom is not null
  |AND S1.http___purl_org_goodrelations_validThrough is not null
  |AND S1.http___schema_org_eligibleQuantity is not null
  |AND S1.http___schema_org_eligibleRegion is not null
  |AND S1.http___schema_org_priceValidUntil is not null
  |""".stripMargin


val s1_prost_csv =
"""
  |SELECT S1.s, S1.http___purl_org_goodrelations_includes, S1.http___purl_org_goodrelations_price,
  |S1.http___purl_org_goodrelations_serialNumber, S1.http___purl_org_goodrelations_validFrom, S1.http___purl_org_goodrelations_validThrough,
  |eligbileRegion, S1.http___schema_org_eligibleQuantity, S1.http___schema_org_priceValidUntil
  |FROM WPT S0
  |JOIN WPT S1
  |ON  array_contains(split(rtrim(']', ltrim('[',S0.http___purl_org_goodrelations_offers)),','), S1.s)
  |AND S0.s='<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer8535>'
  |lateral view explode (split(rtrim(']', ltrim('[',S1.http___schema_org_eligibleRegion)),',')) S1Eligible as eligbileRegion
  |WHERE
  |S1.http___purl_org_goodrelations_includes  is not null
  |AND S1.http___purl_org_goodrelations_price is not null
  |AND S1.http___purl_org_goodrelations_serialNumber is not null
  |AND S1.http___purl_org_goodrelations_validFrom is not null
  |AND S1.http___purl_org_goodrelations_validThrough is not null
  |AND S1.http___schema_org_eligibleQuantity is not null
  |AND S1.http___schema_org_eligibleRegion is not null
  |AND S1.http___schema_org_priceValidUntil is not null
  |""".stripMargin



val s2_prost =
"""
|SELECT  DISTINCT S0.s, S0.http___purl_org_dc_terms_Location, S0.http___db_uwaterloo_ca__galuc_wsdbm_gender
|FROM WPT as S0
|WHERE S0.http___schema_org_nationality="<http://db.uwaterloo.ca/~galuc/wsdbm/Country4>"
|AND  array_contains(S0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type,"<http://db.uwaterloo.ca/~galuc/wsdbm/Role2>")
|AND S0.http___purl_org_dc_terms_Location is not null
|AND S0.http___db_uwaterloo_ca__galuc_wsdbm_gender is not null
|""".stripMargin



val s2_prost_csv =
"""
|SELECT  DISTINCT S0.s, S0.http___purl_org_dc_terms_Location, S0.http___db_uwaterloo_ca__galuc_wsdbm_gender
|FROM WPT as S0
|WHERE S0.http___schema_org_nationality="<http://db.uwaterloo.ca/~galuc/wsdbm/Country4>"
|AND  array_contains(split(rtrim(']', ltrim('[',S0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type)),',')    ,"<http://db.uwaterloo.ca/~galuc/wsdbm/Role2>")
|AND S0.http___purl_org_dc_terms_Location is not null
|AND S0.http___db_uwaterloo_ca__galuc_wsdbm_gender is not null
|""".stripMargin


val s3_prost =
  """
    | SELECT DISTINCT S0.s, S0.http___schema_org_caption, hasGenre, S0.http___schema_org_publisher
    | FROM WPT S0
    |lateral view explode (S0.http___db_uwaterloo_ca__galuc_wsdbm_hasGenre) S0HasGenre as hasGenre
    | WHERE array_contains (S0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type, "<http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory4>")
    | AND S0.http___schema_org_caption is not null
    | AND S0.http___db_uwaterloo_ca__galuc_wsdbm_hasGenre is not null
    | AND S0.http___schema_org_publisher is not null
    |""".stripMargin



val s3_prost_csv =
  """
    | SELECT DISTINCT S0.s, S0.http___schema_org_caption, hasGenre, S0.http___schema_org_publisher
    | FROM WPT S0
    |lateral view explode (split(rtrim(']', ltrim('[',S0.http___db_uwaterloo_ca__galuc_wsdbm_hasGenre)),',')) S0HasGenre as hasGenre
    | WHERE array_contains (split(rtrim(']', ltrim('[',S0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type)),',') , "<http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory4>")
    | AND S0.http___schema_org_caption is not null
    | AND S0.http___db_uwaterloo_ca__galuc_wsdbm_hasGenre is not null
    | AND S0.http___schema_org_publisher is not null
    |""".stripMargin


val s4_prost =
    """
       | SELECT DISTINCT S0.s, S0.http___xmlns_com_foaf_familyName, S3.http___purl_org_ontology_mo_artist
       | FROM WPT S0
       | JOIN WPT S3 ON S3.http___purl_org_ontology_mo_artist=S0.s
       | WHERE S0.http___xmlns_com_foaf_age="<http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup5>"
       | AND S0.http___xmlns_com_foaf_familyName IS NOT NULL
       | AND S0.http___schema_org_nationality="<http://db.uwaterloo.ca/~galuc/wsdbm/Country1>"
       | """.stripMargin

//The same will be from CSV
val s4_prost_csv =
    """
       | SELECT DISTINCT S0.s, S0.http___xmlns_com_foaf_familyName, S3.http___purl_org_ontology_mo_artist
       | FROM WPT S0
       | JOIN WPT S3 ON S3.http___purl_org_ontology_mo_artist=S0.s
       | WHERE S0.http___xmlns_com_foaf_age="<http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup5>"
       | AND S0.http___xmlns_com_foaf_familyName IS NOT NULL
       | AND S0.http___schema_org_nationality="<http://db.uwaterloo.ca/~galuc/wsdbm/Country1>"
       | """.stripMargin


val s5_prost =
  """
    |SELECT DISTINCT S0.s, S0.http___schema_org_description, S0.http___schema_org_keywords
    |FROM WPT S0
    |WHERE array_contains (S0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type, "<http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory3>")
    |AND    array_contains (S0.http___schema_org_language,"<http://db.uwaterloo.ca/~galuc/wsdbm/Language0>")
    |AND S0.http___schema_org_description IS NOT NULL
    |AND S0.http___schema_org_keywords IS NOT NULL
""".stripMargin



val s5_prost_csv =
  """
    |SELECT DISTINCT S0.s, S0.http___schema_org_description, S0.http___schema_org_keywords
    |FROM WPT S0
    |WHERE array_contains (split(rtrim(']', ltrim('[',S0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type)),','),"<http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory3>")
    |AND   array_contains (split(rtrim(']', ltrim('[',S0.http___schema_org_language)),','),"<http://db.uwaterloo.ca/~galuc/wsdbm/Language0>")
    |AND S0.http___schema_org_description IS NOT NULL
    |AND S0.http___schema_org_keywords IS NOT NULL
""".stripMargin



val s6_prost =
  """
    |SELECT DISTINCT S0.s, S0.http___purl_org_ontology_mo_conductor, type
    |FROM WPT S0
    |lateral view explode (S0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type) S0Type as type
    |WHERE   array_contains (S0.http___db_uwaterloo_ca__galuc_wsdbm_hasGenre,"<http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre130>")
    |AND S0.http___purl_org_ontology_mo_conductor is not null
    |AND S0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type  is not null
    |""".stripMargin


val s6_prost_csv =
  """
    |SELECT DISTINCT S0.s, S0.http___purl_org_ontology_mo_conductor, type
    |FROM WPT S0
    |lateral view explode (split(rtrim(']', ltrim('[',S0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type)),',') ) S0Type as type
    |WHERE   array_contains ( split(rtrim(']', ltrim('[',S0.http___db_uwaterloo_ca__galuc_wsdbm_hasGenre)),',') ,"<http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre130>")
    |AND S0.http___purl_org_ontology_mo_conductor is not null
    |AND S0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type  is not null
    |""".stripMargin



 val s7_prost =
      """
        |SELECT DISTINCT T0.s, type, T0.http___schema_org_text
        |FROM WPT T0
        |JOIN WPT T1
        |ON array_contains (T1.http___db_uwaterloo_ca__galuc_wsdbm_likes,T0.s)
        |AND T1.s="<http://db.uwaterloo.ca/~galuc/wsdbm/User54768>"
        |lateral view explode (T0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type) T0Type as type
        |WHERE T0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type IS NOT NULL
        |AND T0.http___schema_org_text IS NOT NULL
        |""".stripMargin


 val s7_prost_csv =
      """
        |SELECT DISTINCT T0.s, type, T0.http___schema_org_text
        |FROM WPT T0
        |JOIN WPT T1
        |ON array_contains (split(rtrim(']', ltrim('[',T1.http___db_uwaterloo_ca__galuc_wsdbm_likes)),',') ,T0.s)
        |AND T1.s="<http://db.uwaterloo.ca/~galuc/wsdbm/User54768>"
        |lateral view explode (split(rtrim(']', ltrim('[',T0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type)),',')) T0Type as type
        |WHERE T0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type IS NOT NULL
        |AND T0.http___schema_org_text IS NOT NULL
        |""".stripMargin










/*
  //Complex

  //100% 16
  val c1 =
    """
    |SELECT DISTINCT V0.Subject, V0.hasReview, V4.reviewer, V7.language
    |FROM WPT V0
    |JOIN WPT V4  ON V0.hasReview=V4.Subject
    |JOIN WPT V6  ON V4.reviewer=V6.Subject
    |JOIN WPT V7  ON V6.Subject=V7.actor
    |WHERE  V0.caption is not null
    |AND V0.sorg_text is not null
    |AND V0.contentRating is not null
    |AND V4.rev_title is not null
    |AND V7.language is not null
  """.stripMargin

  //100%
  val c2 =
    """
      |SELECT DISTINCT V0.Subject, V3.Subject, V4.Subject, V8.Subject
      |FROM WPT V0
      |JOIN WPT V2 ON V0.offers=V2.Subject AND V2.eligibleRegion="http://db.uwaterloo.ca/~galuc/wsdbm/Country3"
      |JOIN WPT V3 ON V2.includes=V3.Subject
      |JOIN WPT V7 ON V7.purchaseFor=V3.Subject
      |JOIN WPT V4 ON V4.makesPurchase=V7.Subject
      |JOIN WPT V8 ON V3.hasReview=V8.Subject
      |WHERE V4.jobTitle is not null
      |AND V0.legalName is not null
      |AND V4.homepage is not null
      |AND V8.totalVotes is not null
      |""".stripMargin


  //90% As Duplicates are there, it should be 434,169, but Distinct gives the right distinct count (805)
  val c3 =
    """
      |SELECT  T0.Subject
      |FROM  WPT T0
      |
      |WHERE T0.likes     is not null
      |AND   T0.friendOf  is not null
      |AND   T0.Location  is not null
      |AND   T0.age       is not null
      |AND   T0.gender    is not null
      |AND   T0.givenName is not null
      |""".stripMargin

  val c3_wpt_prost =
    """
      |SELECT  T0.s
      |FROM WPT T0
      |WHERE T0.http___db_uwaterloo_ca__galuc_wsdbm_likes     is not null
      |AND   T0.http___db_uwaterloo_ca__galuc_wsdbm_friendOf  is not null
      |AND   T0.http___purl_org_dc_terms_Location  is not null
      |AND   T0.http___xmlns_com_foaf_age       is not null
      |AND   T0.http___db_uwaterloo_ca__galuc_wsdbm_gender    is not null
      |AND   T0.http___xmlns_com_foaf_givenName is not null
      |""".stripMargin



  // Snow-Flake (F)

//100%
  val f1 =
    """
      |SELECT DISTINCT V0.Subject, V0.type, V3.Subject, V3.trailer, V3.keywords, V3.trailer
      |FROM WPT V0
      |JOIn WPT V3 ON V3.hasGenre=V0.Subject
      |AND V0.tag="http://db.uwaterloo.ca/~galuc/wsdbm/Topic8"
      |WHERE V3.type="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2"
      |AND V3.keywords is not null
      |AND V3.trailer is not null
      |""".stripMargin

    val f1_prost =
    """
      |SELECT DISTINCT V0.s, V0.http___www_w3_org_1999_02_22_rdf_syntax_ns_type, V3.s, V3.http___schema_org_trailer, V3.http___schema_org_keywords, V3.http___schema_org_trailer
      |FROM WPT V0
      |JOIn WPT V3 ON V3.http___db_uwaterloo_ca__galuc_wsdbm_hasGenre=V0.s
      |AND V0.http___ogp_me_ns_tag="<http://db.uwaterloo.ca/~galuc/wsdbm/Topic8>"
      |WHERE V3.http___www_w3_org_1999_02_22_rdf_syntax_ns_type="<http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2>"
      |AND V3.http___schema_org_keywords is not null
      |AND V3.http___schema_org_trailer is not null
      |""".stripMargin

//100%
  val f2 =
    """
      |SELECT DISTINCT V0.Subject, V0.homepage, V0.title, V0.caption, V0.sorg_description, V1.url, V1.hits
      |FROM WPT V0
      |JOIN WPT V1 ON V0.homepage =V1.Subject
      |WHERE V0.hasGenre="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre117"
      |AND V0.title is not null
      |AND V0.caption is not null
      |AND V0.sorg_description is not null
      |AND V1.url is not null
      |AND V1.hits is not null
      |""".stripMargin

//100%
  val f3 =

    """
      |SELECT DISTINCT V0.Subject, V0.contentRating, V0.contentSize, V4.Subject, V5.Subject, V5.purchaseDate
      |FROM WPT V0
      |JOIN WPT V5 ON V0.Subject=V5.purchaseFor
      |AND V0.hasGenre="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre111"
      |JOIN WPT V4 ON V4.makesPurchase=V5.Subject
      |WHERE V0.contentRating is not null
      |AND V0.contentSize is not null
      |AND V5.purchaseDate is not null
      |""".stripMargin

//100%
  val f4 =

    """
      |SELECT DISTINCT V0.Subject, V1.Subject, V2.Subject, V0.sorg_description, V0.contentSize, V1.url, V1.hits, V7.Subject
      |FROM WPT V0
      |JOIN WPT V1 ON V0.homepage=V1.Subject AND V1.language="http://db.uwaterloo.ca/~galuc/wsdbm/Language0"
      |JOIN WPT V2 ON V0.Subject=V2.includes
      |JOIN WPT V7 ON V0.Subject=V7.likes
      |WHERE V0.tag="http://db.uwaterloo.ca/~galuc/wsdbm/Topic122"
      |AND V0.sorg_description is not null
      |AND V0.contentSize is not null
      |AND V1.url is not null
      |AND V1.hits is not null
      |""".stripMargin

//100%
  val f5 =
    """
      |SELECT DISTINCT V0.Subject, V1.Subject, V0.price, V0.validThrough, V1.title, V1.type
      |FROM WPT V0
      |JOIN WPT V1 ON V0.includes=V1.Subject
      |JOIN WPT V2 ON V2.offers= V0.Subject
      |AND V2.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/Retailer9885"
      |WHERE V0.price is not null
      |AND V0.validThrough is not null
      |AND V1.title is not null
      |AND V1.type is not null
      |""".stripMargin


  // Linear (L)

//100%
  val l1 =
    """
      |SELECT DISTINCT T0.Subject, T1.Subject, T1.caption
      |FROM WPT T0
      |JOIN WPT T1 ON T0.likes=T1.Subject
      |WHERE T0.Subscribes="http://db.uwaterloo.ca/~galuc/wsdbm/Website7355"
      |AND T1.caption  is not null
      |""".stripMargin


  //100%
  val l2 =
       """
      |SELECT DISTINCT T_User.Subject, TT.parentCountry
      |FROM WPT T_User
      |JOIN (SELECT T_City.parentCountry FROM WPT T_City
      |WHERE T_City.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/City70") TT
      |ON TT.parentCountry=T_User.nationality
      |WHERE T_User.likes="http://db.uwaterloo.ca/~galuc/wsdbm/Product0"
      |""".stripMargin


//100%
  val l3 =
    """
      |SELECT DISTINCT T0.Subject, T0.likes
      |FROM WPT T0
      |WHERE T0.subscribes="http://db.uwaterloo.ca/~galuc/wsdbm/Website43164"
      |AND T0.likes is not null
      |""".stripMargin

//100%
  val l4 =
    """
      |SELECT  DISTINCT T0.Subject, T0.caption
      |FROM WPT T0
      |WHERE T0.tag="http://db.uwaterloo.ca/~galuc/wsdbm/Topic142"
      |AND T0.caption is not null
      |""".stripMargin

  //100%
    val l5 =
       """
      |SELECT DISTINCT T_User.Subject, T_User.JobTitle, TT.parentCountry
      |FROM WPT T_User
      |JOIN (SELECT T_City.parentCountry FROM WPT T_City
      |WHERE T_City.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/City40") TT
      |ON TT.parentCountry=T_User.nationality
      |WHERE T_User.JobTitle is not null
      |""".stripMargin


  //Star (S) 100%

  //100%
  val s1 =
    """
      |SELECT S1.SUBJECT, S0.Includes, S1.PRICE, S1.SERIALNUMBER, S1.VALIDFROM, S1.VALIDTHROUGH,
      |S1.ELIGIBLEQUANTITY, S1.ELIGIBLEREGION, S1.PRICEVALIDUNTIL
      |FROM WPT S0
      |JOIN WPT S1
      |ON S1.SUBJECT=S0.Offers
      |AND S0.Subject='http://db.uwaterloo.ca/~galuc/wsdbm/Retailer8535'
      |WHERE
      |S1.INCLUDES  is not null
      |AND S1.PRICE is not null
      |AND S1.SERIALNUMBER is not null
      |AND S1.VALIDFROM is not null
      |AND S1.VALIDTHROUGH is not null
      |AND S1.ELIGIBLEQUANTITY is not null
      |AND S1.ELIGIBLEREGION is not null
      |AND S1.PRICEVALIDUNTIL is not null
      |""".stripMargin


    val s1_prost =
    """
      |SELECT S1.s, S0.http___purl_org_goodrelations_includes, S1.http___purl_org_goodrelations_price,
      |S1.http___purl_org_goodrelations_serialNumber, S1.http___purl_org_goodrelations_validFrom, S1.http___purl_org_goodrelations_validThrough,
      |S1.http___schema_org_eligibleQuantity, S1.http___schema_org_eligibleRegion, S1.http___schema_org_priceValidUntil
      |FROM WPT S0
      |JOIN WPT S1
      |ON S1.s=S0.http___purl_org_goodrelations_offers
      |AND S0.s='<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer8535>'
      |WHERE
      |S1.http___purl_org_goodrelations_includes  is not null
      |AND S1.http___purl_org_goodrelations_price is not null
      |AND S1.http___purl_org_goodrelations_serialNumber is not null
      |AND S1.http___purl_org_goodrelations_validFrom is not null
      |AND S1.http___purl_org_goodrelations_validThrough is not null
      |AND S1.http___schema_org_eligibleQuantity is not null
      |AND S1.http___schema_org_eligibleRegion is not null
      |AND S1.http___schema_org_priceValidUntil is not null
      |""".stripMargin



//100%
    val s2 =
      """
        |SELECT  DISTINCT WPT.Subject, WPT.Location, WPT.gender
        |FROM WPT
        |WHERE WPT.nationality="http://db.uwaterloo.ca/~galuc/wsdbm/Country4"
        |AND WPT.type="http://db.uwaterloo.ca/~galuc/wsdbm/Role2"
        |AND WPT.Location is not null
        |AND WPT.gender is not null
        |""".stripMargin
//100%
    val s3 =
      """
        | SELECT DISTINCT WPT.Subject, WPT.caption, WPT.hasGenre, WPT.publisher
        | FROM WPT
        | WHERE WPT.type="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory4"
        | AND WPT.caption is not null
        | AND WPT.hasGenre is not null
        | AND WPT.publisher is not null
        |""".stripMargin

//100%
    val s4 =
      """
         | SELECT DISTINCT S0.SUBJECT, S0.FAMILYNAME, S3.ARTIST
         | FROM WPT S0
         | JOIN WPT S3 ON S3.artist=S0.Subject
         | WHERE S0.AGE = "http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup5"
         | AND S0.FAMILYNAME IS NOT NULL
         | AND S0.NATIONALITY="http://db.uwaterloo.ca/~galuc/wsdbm/Country1"
         | """.stripMargin

//100%
    val s5 =
      """
        |SELECT DISTINCT WPT.SUBJECT, WPT.sorg_description, WPT.KEYWORDS
        |FROM WPT
        |WHERE WPT.TYPE="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory3"
        |AND WPT.LANGUAGE="http://db.uwaterloo.ca/~galuc/wsdbm/Language0"
        |AND WPT.sorg_description IS NOT NULL
        |AND WPT.KEYWORDS IS NOT NULL
    """.stripMargin

//100%
    val s6 =
      """
        |SELECT DISTINCT WPT.Subject, WPT.conductor, WPT.type
        |FROM WPT
        |WHERE WPT.hasgenre="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre130"
        |AND WPT.conductor is not null
        |AND WPT.type  is not null
        |""".stripMargin

//100%
    val s7 =
      """
        |SELECT DISTINCT T0.SUBJECT, T0.TYPE, T0.sorg_text
        |FROM WPT T0
        |JOIN WPT T1 ON T1.likes=T0.Subject
        |AND T1.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/User54768"
        |WHERE T0.TYPE IS NOT NULL
        |AND T0.sorg_text IS NOT NULL
        |""".stripMargin

  */


}