package ee.ut.cs.bigdata.watdiv.querying.queries

class WPTQueries {
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


}