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


  val c2 =
    """

""".stripMargin


  val c3 =
    """

  """.stripMargin



  // Snow-Flake (F)


  val q4 =
    """

    """.stripMargin


  val q5 =
    """

    """.stripMargin


  val q6 =

    """

  """.stripMargin


  val q7 =

    """

    """.stripMargin


  val q8 =
    """

    """.stripMargin


  // Linear (L)


  val q9 =
    """

    """.stripMargin


  val q10 =
    """

    """.stripMargin


  val q11 =
    """

    """.stripMargin


  val q12 =
    """

    """.stripMargin


  val q13 =
    """

    """.stripMargin


  //Star (S)


  val q14 =
    """

    """.stripMargin


  val q15 =
    """

    """.stripMargin


  val q16 =
    """

    """.stripMargin


  val q17 =
    """

    """.stripMargin


  val q18 =
    """

    """.stripMargin


  val q19 =
    """

    """.stripMargin


  val q20 =
    """

    """.stripMargin
}
