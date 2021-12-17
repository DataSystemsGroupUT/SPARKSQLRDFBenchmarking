package ee.ut.cs.bigdata.watdiv.querying.queries

class ExtVPQueries {

//Complex

  val c1 =
    """
      |SELECT tab0.v1 AS v1 , tab0.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2
      |FROM    (SELECT obj AS v1 , sub AS v0
      |FROM SS_caption_hasReview
      |) tab0
      |JOIN    (SELECT sub AS v0 , obj AS v3
      |FROM SS_contentRating_caption
      |) tab2
      |ON(tab0.v0=tab2.v0)
      |JOIN    (SELECT sub AS v0 , obj AS v2
      |FROM SS_text_caption
      |) tab1
      |ON(tab2.v0=tab1.v0)
      |JOIN    (SELECT sub AS v0 , obj AS v4
      |FROM SS_hasReview_caption
      |) tab3 ON(tab1.v0=tab3.v0)
      |JOIN    (SELECT obj AS v5 , sub AS v4 FROM SO_title_hasReview) tab4
      |ON(tab3.v4=tab4.v4)
      |JOIN    (SELECT obj AS v6 , sub AS v4
      |FROM SS_reviewer_title) tab5
      |ON(tab4.v4=tab5.v4)
      |JOIN    (SELECT sub AS v7 , obj AS v6
      |FROM SS_actor_language
      |) tab6
      |ON(tab5.v6=tab6.v6)
      |JOIN    (SELECT sub AS v7 , obj AS v8
      |FROM SS_language_actor
      |) tab7
      |ON(tab6.v7=tab7.v7)
      |""".stripMargin


  val c1_copy =
    """
      |SELECT tab0.v1 AS v1 , tab0.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2
      |FROM    (SELECT obj AS v1 , sub AS v0
      |FROM SS_caption_hasReview
      |) tab0
      |JOIN    (SELECT sub AS v0 , obj AS v3
      |FROM SS_contentRating_caption
      |) tab2
      |ON(tab0.v0=tab2.v0)
      |JOIN    (SELECT sub AS v0 , obj AS v2
      |FROM SS_text_caption
      |) tab1
      |ON(tab2.v0=tab1.v0)
      |JOIN    (SELECT sub AS v0 , obj AS v4
      |FROM SS_hasReview_caption
      |) tab3 ON(tab1.v0=tab3.v0)
      |JOIN    (SELECT obj AS v5 , sub AS v4 FROM VP_Rev_title) tab4
      |ON(tab3.v4=tab4.v4)
      |JOIN    (SELECT obj AS v6 , sub AS v4
      |FROM VP_Reviewer) tab5
      |ON(tab4.v4=tab5.v4)
      |JOIN    (SELECT sub AS v7 , obj AS v6
      |FROM SS_actor_language
      |) tab6
      |ON(tab5.v6=tab6.v6)
      |JOIN    (SELECT sub AS v7 , obj AS v8
      |FROM SS_language_actor
      |) tab7
      |ON(tab6.v7=tab7.v7)
      |""".stripMargin


val c2 =
    """
      |SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab4.v5 AS v5 , tab7.v7 AS v7 , tab5.v6 AS v6 , tab6.v4 AS v4 , tab9.v9 AS v9 , tab3.v3 AS v3 , tab8.v8 AS v8 , tab2.v2 AS v2
      |FROM    (SELECT sub AS v2
      |FROM SO_eligibleRegion_offers
      |WHERE obj = 'wsdbm:Country5'
 ) tab2
 JOIN    (SELECT sub AS v0 , obj AS v2
  FROM SS_offers_legalName
 ) tab1
 ON(tab2.v2=tab1.v2)
 JOIN    (SELECT obj AS v1 , sub AS v0
  FROM VP_legalName

 ) tab0
 ON(tab1.v0=tab0.v0)
 JOIN    (SELECT obj AS v3 , sub AS v2
  FROM OS_includes_hasReview
 ) tab3
 ON(tab1.v2=tab3.v2)
 JOIN    (SELECT sub AS v3 , obj AS v8
  FROM OS_hasReview_totalVotes

 ) tab8
 ON(tab3.v3=tab8.v3)
 JOIN    (SELECT obj AS v9 , sub AS v8
  FROM SO_totalVotes_hasReview

 ) tab9
 ON(tab8.v8=tab9.v8)
 JOIN    (SELECT sub AS v7 , obj AS v3
  FROM OS_purchaseFor_hasReview

 ) tab7
 ON(tab8.v3=tab7.v3)
 JOIN    (SELECT obj AS v7 , sub AS v4
  FROM SS_makesPurchase_homepage

 ) tab6
 ON(tab7.v7=tab6.v7)
 JOIN    (SELECT obj AS v5 , sub AS v4
  FROM SS_jobTitle_homepage

 ) tab4
 ON(tab6.v4=tab4.v4)
 JOIN    (SELECT obj AS v6 , sub AS v4
  FROM SS_homepage_jobTitle


 ) tab5
 ON(tab4.v4=tab5.v4)
      |""".stripMargin


  val c3 =
    """
      |SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2
 FROM    (SELECT sub AS v0 , obj AS v3
  FROM SS_Location_likes
 ) tab2
 JOIN    (SELECT sub AS v0 , obj AS v4
  FROM SS_age_likes
 ) tab3
 ON(tab2.v0=tab3.v0)
 JOIN    (SELECT sub AS v0 , obj AS v5
  FROM SS_gender_likes

 ) tab4
 ON(tab3.v0=tab4.v0)
 JOIN    (SELECT sub AS v0 , obj AS v6
  FROM SS_givenName_likes

 ) tab5
 ON(tab4.v0=tab5.v0)
 JOIN    (SELECT obj AS v1 , sub AS v0
  FROM SS_likes_Location

 ) tab0
 ON(tab5.v0=tab0.v0)
 JOIN    (SELECT sub AS v0 , obj AS v2
  FROM SS_friendOf_likes
 ) tab1
 ON(tab0.v0=tab1.v0)

      |""".stripMargin



  // Snow-Flake (F)


  val F1 =
    """

    """.stripMargin


  val F2 =
    """

    """.stripMargin


  val F3 =

    """

  """.stripMargin


  val F4 =

    """

    """.stripMargin


  val F5 =
    """

    """.stripMargin


  // Linear (L)


  val L1 =
    """

    """.stripMargin


  val L2 =
    """

    """.stripMargin


  val L3 =
    """

    """.stripMargin


  val L4 =
    """

    """.stripMargin


  val L5 =
    """

    """.stripMargin


  //Star (S)


  val S1 =
    """

    """.stripMargin


  val S2 =
    """

    """.stripMargin


  val S3 =
    """

    """.stripMargin


  val S4 =
    """

    """.stripMargin


  val S5 =
    """

    """.stripMargin


  val S6 =
    """

    """.stripMargin


  val S7 =
    """

    """.stripMargin
}