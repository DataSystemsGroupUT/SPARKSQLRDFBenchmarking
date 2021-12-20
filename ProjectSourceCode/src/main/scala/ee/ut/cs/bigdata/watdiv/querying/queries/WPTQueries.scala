package ee.ut.cs.bigdata.watdiv.querying.queries

class WPTQueries {
  //Complex

  val c1 =
    """
    |SELECT DISTINCT
    |FROM WPT V0
    |JOIN WPT V4
    |ON V0.HASREVIEW IS NOT NULL
    |AND
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
      |SELECT S1.SUBJECT, S0.SUBJECT, S1.PRICE, S1.SERIALNUMBER, S1.VALIDFROM, S1.VALIDTHROUGH,
      |S1.ELIGIBLEQUANTITY, S1.ELIGIBLEREGION, S1.PRICEVALIDUNTIL
      |FROM WPT S0
      |JOIN WPT S1
      |ON S1.SUBJECT=S0.Offers
      |AND S1.Subject='http://db.uwaterloo.ca/~galuc/wsdbm/Retailer4'
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


    val s2 =
      """
        |SELECT  DISTINCT WPT.Subject, WPT.Location, WPT.gender
        |FROM WPT
        |WHERE WPT.nationality="http://db.uwaterloo.ca/~galuc/wsdbm/Country4"
        |AND WPT.type="http://db.uwaterloo.ca/~galuc/wsdbm/Role2"
        |AND WPT.Location is not null
        |AND WPT.gender is not null
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
         | SELECT DISTINCT WPT.SUBJECT, WPT.FAMILYNAME,WPT.ARTIST
         | FROM WPT
         | WHERE WPT.AGE = "http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup1"
         | AND WPT.FAMILYNAME IS NOT NULL
         | AND WPT.ARTIST IS NOT NULL
         | AND WPT.NATIONALITY="http://db.uwaterloo.ca/~galuc/wsdbm/Country1"
         |
         |

    """.stripMargin


    val s5 =
      """
        |SELECT DISTINCT WPT.SUBJECT, WPT.sorg_description, WPT.KEYWORDS
        |FROM WPT
        |WHERE WPT.TYPE="http://www.w3.org/1999/02/22-rdf-syntax-ns#/ProductCategory9"
        |AND WPT.LANGUAGE="http://db.uwaterloo.ca/~galuc/wsdbm/Language0"
        |AND WPT.sorg_description IS NOT NULL
        |AND WPT.KEYWORDS IS NOT NULL
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
        |SELECT DISTINCT WPT.SUBJECT, WPT.TYPE, WPT.sorg_text
        |FROM WPT
        |WHERE WPT.LIKES="http://db.uwaterloo.ca/~galuc/wsdbm/User828"
        |AND WPT.TYPE IS NOT NULL
        |AND WPT.sorg_text IS NOT NULL
        |""".stripMargin


}