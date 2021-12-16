package ee.ut.cs.bigdata.watdiv.querying.queries

class WPTQueries {
  //Complex

  val c1 =
    """

  """.stripMargin


  val c2 =
    """

""".stripMargin


  val c3 =
    """

  """.stripMargin



  // Snow-Flake (F)


  val f1 =
    """

    """.stripMargin


  val f2 =
    """

    """.stripMargin


  val f3 =

    """

  """.stripMargin


  val f4 =

    """

    """.stripMargin


  val f5 =
    """

    """.stripMargin


  // Linear (L)


  val l1 =
    """

    """.stripMargin


  val l2 =
    """

    """.stripMargin


  val l3 =
    """

    """.stripMargin


  val l4 =
    """

    """.stripMargin


  val l5 =
    """

    """.stripMargin


  //Star (S)


  val s1 =
    """
      |SELECT WPT.SUBJECT, WPT.INCLUDES, WPT.PRICE, WPT.SERIALNUMBER, WPT.VALIDFROM, WPT.VALIDTHROUGH,
      |WPT.ELIGIBLEQUANTITY, WPT.ELIGIBLEREGION, WPT.PRICEVALIDUNTIL
      |FROM WPT
      |WHERE WPT.OFFERS='http://db.uwaterloo.ca/~galuc/wsdbm/Retailer107'
      |AND WPT.INCLUDES  is not null
      |AND WPT.PRICE is not null
      |AND WPT.SERIALNUMBER is not null
      |AND WPT.VALIDFROM is not null
      |AND WPT.VALIDTHROUGH is not null
      |AND WPT.ELIGIBLEQUANTITY is not null
      |AND WPT.ELIGIBLEREGION is not null
      |AND WPT.PRICEVALIDUNTIL is not null
      |""".stripMargin


    val s2 =
      """
        |SELECT  WPT.Subject, WPT.Location, WPT.gender
        |FROM WPT
        |WHERE WPT.nationality="http://db.uwaterloo.ca/~galuc/wsdbm/Country4"
        |AND WPT.type="http://db.uwaterloo.ca/~galuc/wsdbm/Role2"
        |""".stripMargin

    val s3 =
      """
        | SELECT DISTINCT WPT.Subject, WPT.caption, WPT.hasGenre, WPT.publisher
        | FROM WPT
        | WHERE WPT.type="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory4"
        | AND WPT.caption is not null
        | AND WPT.hasGenre is not null
        | AND WPT.publisher is not null
        |""".stripMargin


    val s4 =
      """

    """.stripMargin


    val s5 =
      """

    """.stripMargin


    val s6 =
      """
        |SELECT DISTINCT WPT.Subject, WPT.conductor, WPT.type
        |FROM WPT
        |WHERE WPT.hasgenre="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre115"
        |AND WPT.conductor is not null
        |AND WPT.type  is not null
        |""".stripMargin


    val s7 =
      """
        |
        |""".stripMargin


}

