package ee.ut.cs.bigdata.watdiv.querying.queries

class WPTQueries {
  //Complex

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


  val c2 =
    """

""".stripMargin


  val c3 =
    """

  """.stripMargin



  // Snow-Flake (F)

//100%
  val f1 =
    """
      |SELECT DISTINCT V0.Subject, V0.type, V3.Subject, V3.trailer, V3.keywords, V3.trailer
      |FROM WPT V0
      |JOIn WPT V3 ON V3.hasGenre=V0.Subject
      |AND V0.tag="http://db.uwaterloo.ca/~galuc/wsdbm/Topic47"
      |WHERE V3.type="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2"
      |AND V3.keywords is not null
      |AND V3.trailer is not null
      |""".stripMargin

//100%
  val f2 =
    """
      |SELECT DISTINCT V0.Subject, V0.homepage, V0.title, V0.caption, V0.sorg_description, V1.url, V1.hits
      |FROM WPT V0
      |JOIN WPT V1 ON V0.homepage =V1.Subject
      |WHERE V0.hasGenre="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre62"
      |AND V0.title is not null
      |AND V0.caption is not null
      |AND V0.sorg_description is not null
      |AND V1.url is not null
      |AND V1.hits is not null
      |""".stripMargin


  val f3 =

    """
      |SELECT DISTINCT V0.Subject, V0.contentRating, V0.contentSize, V4.Subject, V5.Subject, V5.purchaseDate
      |FROM WPT V0
      |JOIN WPT V5 ON V0.Subject=V5.purchaseFor
      |AND V0.hasGenre="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre131"
      |JOIN WPT V4 ON V4.makesPurchase=V5.Subject
      |WHERE V0.contentRating is not null
      |AND V0.contentSize is not null
      |AND V5.purchaseDate is not null
      |""".stripMargin


  val f4 =

    """
      |SELECT DISTINCT V0.Subject, V1.Subject, V2.Subject, V0.sorg_description, V0.contentSize, V1.url, V1.hits, V7.Subject
      |FROM WPT V0
      |JOIN WPT V1 ON V0.homepage=V1.Subject AND V1.language="http://db.uwaterloo.ca/~galuc/wsdbm/Language0"
      |JOIN WPT V2 ON V0.Subject=V2.includes
      |JOIN WPT V7 ON V0.Subject=V7.likes
      |WHERE V0.tag="http://db.uwaterloo.ca/~galuc/wsdbm/Topic52"
      |AND V0.sorg_description is not null
      |AND V0.contentSize is not null
      |AND V1.url is not null
      |AND V1.hits is not null
      |""".stripMargin


  val f5 =
    """
      |SELECT DISTINCT V0.Subject, V1.Subject, V0.price, V0.validThrough, V1.title, V1.type
      |FROM WPT V0
      |JOIN WPT V1 ON V0.includes=V1.Subject
      |JOIN WPT V2 ON V2.offers= V0.Subject AND V2.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/Retailer10"
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
      |WHERE T0.Subscribes="http://db.uwaterloo.ca/~galuc/wsdbm/Website30"
      |AND T1.caption  is not null
      |""".stripMargin


  //
  val l2 =
    """
      |SELECT DISTINCT T1.Subject, T0.Subject
      |FROM WPT T0
      |JOIN WPT T1 ON T0.nationality=T1.Subject
      |JOIN WPT T2 ON T1.Subject=T2.parentCountry
      |WHERE T0.likes="http://db.uwaterloo.ca/~galuc/wsdbm/Product0"
      |AND T2.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/City152"
      |""".stripMargin

//100%
  val l3 =
    """
      |SELECT DISTINCT T0.Subject, T0.likes
      |FROM WPT T0
      |WHERE T0.subscribes="http://db.uwaterloo.ca/~galuc/wsdbm/Website34"
      |AND T0.likes is not null
      |""".stripMargin

//100%
  val l4 =
    """
      |SELECT  DISTINCT T0.Subject, T0.caption
      |FROM WPT T0
      |WHERE T0.tag="http://db.uwaterloo.ca/~galuc/wsdbm/Topic24"
      |AND T0.caption is not null
      |""".stripMargin


  val l5 =
    """
      |SELECT  T0.Subject, T0.JobTitle, T1.Subject
      |FROM WPT T0
      |JOIN WPT T1 ON T0.nationality= T1.Subject
      |JOIN WPT T2 ON T1.subject=T2.parentCountry
      |WHERE T2.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/City187"
      |""".stripMargin


  //Star (S)

  val s1 =
    """
      |SELECT S1.SUBJECT, S0.Includes, S1.PRICE, S1.SERIALNUMBER, S1.VALIDFROM, S1.VALIDTHROUGH,
      |S1.ELIGIBLEQUANTITY, S1.ELIGIBLEREGION, S1.PRICEVALIDUNTIL
      |FROM WPT S0
      |JOIN WPT S1
      |ON S1.SUBJECT=S0.Offers
      |AND S0.Subject='http://db.uwaterloo.ca/~galuc/wsdbm/Retailer4'
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
         | SELECT DISTINCT SO.SUBJECT, S0.FAMILYNAME,WPT.ARTIST
         | FROM WPT S0
         | JOIN WPT S3
         | ON S3.SUBJECT=S0.ARTIST
         | WHERE S0.AGE = "http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup1"
         | AND S0.FAMILYNAME IS NOT NULL
         | AND S0.ARTIST IS NOT NULL
         | AND S0.NATIONALITY="http://db.uwaterloo.ca/~galuc/wsdbm/Country1"
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