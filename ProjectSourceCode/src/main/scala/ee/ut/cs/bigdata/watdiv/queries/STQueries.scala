package ee.ut.cs.bigdata.watdiv.queries

class STQueries {


  //Complex

  val q1 =
    """
      |SELECT T1.Subject, T4.Object, T6.Object , T7.Subject
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
      |INNER JOIN Triples T4 ON T3.Subject=T4.Subject
      |INNER JOIN Triples T5 ON T4.Object=T5.Subject
      |INNER JOIN Triples T6 ON T5.Subject=T6.Subject
      |INNER JOIN Triples T7 ON T6.Object=T7.Object
      |INNER JOIN Triples T8 ON T7.Subject=T8.Subject
      |
      |WHERE
      |T1.Predicate="http://schema.org/caption"
      |AND T2.Predicate="http://schema.org/text"
      |AND T3.Predicate="http://schema.org/contentRating"
      |AND T4.Predicate="http://purl.org/stuff/rev#hasReview"
      |AND T5.Predicate="http://purl.org/stuff/rev#title"
      |AND T6.Predicate="http://purl.org/stuff/rev#reviewer"
      |AND T7.Predicate="http://schema.org/actor"
      |AND T8.Predicate="http://schema.org/language"
  """.stripMargin


  val q2 =
    """
      |SELECT T1.Object, T4.Object, T6.Subject , T9.Object
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |INNER JOIN Triples T3 ON T2.Object=T3.Subject
      |INNER JOIN Triples T4 ON T3.Subject=T4.Subject
      |INNER JOIN Triples T5 ON T4.Object=T5.Object
      |INNER JOIN Triples T6 ON T5.Subject=T6.Object
      |INNER JOIN Triples T7 ON T6.Subject=T7.Subject
      |INNER JOIN Triples T8 ON T7.Subject=T8.Subject
      |INNER JOIN Triples T9 ON T4.Object=T9.Subject
      |INNER JOIN Triples T10 ON T9.Object=T10.Subject
      |
      |WHERE
      |T1.Predicate="http://schema.org/legalName"
      |AND T2.Predicate="http://purl.org/goodrelations/offers"
      |AND T3.Predicate="http://schema.org/eligibleRegion"
      |AND T3.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Country3"
      |AND T4.Predicate="http://purl.org/goodrelations/includes"
      |AND T5.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor"
      |AND T6.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/makesPurchase"
      |AND T7.Predicate="http://xmlns.com/foaf/homepage"
      |AND T8.Predicate="http://schema.org/jobTitle"
      |AND T9.Predicate="http://purl.org/stuff/rev#hasReview"
      |AND T10.Predicate="http://purl.org/stuff/rev#totalVotes"
""".stripMargin


  val q3 =
    """
      |SELECT T1.Subject
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
      |INNER JOIN Triples T4 ON T3.Subject=T4.Subject
      |INNER JOIN Triples T5 ON T4.Subject=T5.Subject
      |INNER JOIN Triples T6 ON T5.Subject=T6.Subject
      |
      |WHERE
      |T1.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/likes"
      |AND T2.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/friendOf"
      |AND T3.Predicate="http://purl.org/dc/terms/Location"
      |AND T4.Predicate="http://xmlns.com/foaf/age"
      |AND T5.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/gender"
      |AND T6.Predicate="http://xmlns.com/foaf/givenName"
  """.stripMargin



   val q4 =
    """
      |SELECT T1.Subject, T1.Object, T3.Subject, T5.Object, T6.Object
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |INNER JOIN Triples T3 ON T2.Subject=T3.Object
      |INNER JOIN Triples T4 ON T3.Subject=T4.Subject
      |INNER JOIN Triples T5 ON T4.Subject=T5.Subject
      |INNER JOIN Triples T6 ON T5.Subject=T6.Subject
      |
      |WHERE
      |T1.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      |AND T2.Predicate="http://ogp.me/ns#tag"
      |AND T2.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Topic49"
      |ANd T3.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre"
      |AND T4.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      |AND T4.Object="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2"
      |AND T5.Predicate="http://schema.org/keywords"
      |AND T6.Predicate="http://schema.org/trailer"
    """.stripMargin


  val q5 =
    """
      |SELECT T1.Subject, T1.Object, T2.Object, T4.Object, T6.Object, T7.Object, T8.Object
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
      |INNER JOIN Triples T4 ON T3.Subject=T4.Subject
      |INNER JOIN Triples T5 ON T4.Subject=T5.Subject
      |INNER JOIN Triples T6 ON T5.Subject=T6.Subject
      |INNER JOIN Triples T7 ON T6.Object=T7.Subject
      |INNER JOIN Triples T8 ON T7.Subject=T8.Subject
      |
      |WHERE
      |T1.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      |AND T2.Predicate="http://ogp.me/ns#title"
      |AND T3.Predicate="http://schema.org/caption"
      |AND T4.Predicate="http://schema.org/description"
      |AND T5.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre"
      |AND T5.Object="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre117"
      |AND T6.Predicate="http://xmlns.com/foaf/homepage"
      |AND T7.Predicate="http://schema.org/url"
      |AND T8.Predicate ="http://db.uwaterloo.ca/~galuc/wsdbm/hits"
    """.stripMargin


  val q6 =

    """
      |SELECT T1.Subject, T1.Object, T2.Object, T4.Object, T5.Object,T6.Object
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
      |INNER JOIN Triples T4 ON T3.Subject=T4.Object
      |INNER JOIN Triples T5 ON T4.Subject=T5.Subject
      |INNER JOIN Triples T6 ON T5.Subject=T6.Object
      |
      |WHERE
      |T1.Predicate="http://schema.org/contentRating"
      |AND T2.Predicate="http://schema.org/contentSize"
      |ANd T3.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre"
      |AND T3.Object="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre111"
      |AND T4.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor"
      |AND T5.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/purchaseDate"
      |AND T6.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/makesPurchase"
  """.stripMargin


  val q7 =

    """
      |SELECT T1.Subject, T2.Object, T3.Object, T4.Subject, T5.Object, T6.Object, T7.Object, T9.Object
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
      |INNER JOIN Triples T4 ON T3.Subject=T4.Object
      |INNER JOIN Triples T5 ON T3.Subject=T5.Subject
      |INNER JOIN Triples T6 ON T5.Object=T6.Subject
      |INNER JOIN Triples T7 ON T3.Subject=T7.Object
      |INNER JOIN Triples T8 ON T6.Subject=T8.Subject
      |INNER JOIN Triples T9 ON T8.Subject=T9.Subject
      |
      |WHERE
      |T1.Predicate="http://ogp.me/ns#tag"
      |AND T1.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Topic122"
      |AND T2.Predicate="http://schema.org/description"
      |AND T3.Predicate="http://schema.org/contentSize"
      |AND T4.Predicate="http://purl.org/goodrelations/includes"
      |AND T5.Predicate="http://xmlns.com/foaf/homepage"
      |AND T6.Predicate="http://schema.org/url"
      |AND T7.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/likes"
      |AND T8.Predicate ="http://schema.org/language"
      |AND T8.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Language0"
      |AND T9.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/hits"
    """.stripMargin


  val q8 =
    """
      |SELECT T1.Object, T2.Object, T3.Object, T4.Object, T5.Object, T6.Object
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Object=T2.Subject
      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
      |INNER JOIN Triples T4 ON T3.Subject=T4.Subject
      |INNER JOIN Triples T5 ON T4.Object=T5.Subject
      |INNER JOIN Triples T6 ON T5.Subject=T6.Subject
      |
      |WHERE
      |T1.Predicate="http://purl.org/goodrelations/offers"
      |AND T1.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/Retailer9885"
      |AND T2.Predicate="http://purl.org/goodrelations/price"
      |AND T3.Predicate="http://purl.org/goodrelations/validThrough"
      |AND T4.Predicate="http://purl.org/goodrelations/includes"
      |AND T5.Predicate="http://ogp.me/ns#title"
      |AND T6.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    """.stripMargin


  // Linear (L)

  val q9 =
    """
      |SELECT T1.Subject,T3.Subject,T3.Object
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |INNER JOIN Triples T3 ON T2.Object=T3.Subject
      |WHERE T1.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/subscribes"
      |AND   T1.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Website7355"
      |AND   T3.Predicate="http://schema.org/caption"
    """.stripMargin


  val q10 =
    """
      |SELECT T1.Subject,T3.Subject
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |INNER JOIN Triples T3 ON T2.Object=T3.Object
      |WHERE T1.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/likes"
      |AND   T1.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Product0"
      |AND   T2.Predicate="http://schema.org/nationality"
      |AND   T3.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/City70"
      |AND   T3.Predicate="http://www.geonames.org/ontology#parentCountry"
    """.stripMargin


  val q11 =
    """
      |SELECT T1.Subject, T2.Object
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |WHERE T1.Predicate ="http://db.uwaterloo.ca/~galuc/wsdbm/subscribes"
      |AND T1.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Website43164"
      |AND T2.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/likes"
    """.stripMargin


  val q12 =
    """
      |SELECT T1.Subject, T2.Object
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |WHERE T1.Predicate ='http://ogp.me/ns#tag'
      |AND T1.Object='http://db.uwaterloo.ca/~galuc/wsdbm/Topic142'
      |AND T2.Predicate='http://schema.org/caption'
      |""".stripMargin


  val q13 =
    """
      |SELECT  T1.Subject, T1.Object,  T2.Object
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |INNER JOIN Triples T3 ON T2.Object=T3.Object
      |WHERE T1.Predicate="http://schema.org/jobTitle"
      |AND   T2.Predicate="http://schema.org/nationality"
      |AND   T3.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/City40"
      |AND   T3.Predicate="http://www.geonames.org/ontology#parentCountry"
    """.stripMargin


  //Star (S)


  val q14 =
    """
      |SELECT T1.Subject, T1.Object , T3.Object, T4.Object, T5.Object, T6.Object,T7.Object,T8.Object,T9.Object
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Object
      |INNER JOIN Triples T3 ON T1.Subject=T3.Subject
      |INNER JOIN Triples T4 ON T1.Subject=T4.Subject
      |INNER JOIN Triples T5 ON T1.Subject=T5.Subject
      |INNER JOIN Triples T6 ON T1.Subject=T6.Subject
      |INNER JOIN Triples T7 ON T1.Subject=T7.Subject
      |INNER JOIN Triples T8 ON T1.Subject=T8.Subject
      |INNER JOIN Triples T9 ON T1.Subject=T9.Subject
      |
      |WHERE T1.Predicate="http://purl.org/goodrelations/includes"
      |AND T2.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/Retailer8535"
      |AND T2.Predicate="http://purl.org/goodrelations/offers"
      |AND T3.Predicate="http://purl.org/goodrelations/price"
      |AND T4.Predicate="http://purl.org/goodrelations/serialNumber"
      |AND T5.Predicate="http://purl.org/goodrelations/validFrom"
      |AND T6.Predicate="http://purl.org/goodrelations/validThrough"
      |AND T7.Predicate="http://schema.org/eligibleQuantity"
      |AND T8.Predicate="http://schema.org/eligibleRegion"
      |AND T9.Predicate="http://schema.org/priceValidUntil"
    """.stripMargin


  val q15 =
    """
      |SELECT T1.Subject, T1.Object, T3.Object
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
      |INNER JOIN Triples T4 ON T3.Subject=T4.Subject
      |WHERE T1.Predicate="http://purl.org/dc/terms/Location"
      |AND T2.Predicate="http://schema.org/nationality"
      |AND T2.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Country4"
      |AND T3.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/gender"
      |AND T4.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      |AND T4.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Role2"
    """.stripMargin


  val q16 =
    """
      |SELECT T1.Subject, T2.Object, T3.Object,T4.Object
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
      |INNER JOIN Triples T4 ON T3.Subject=T4.Subject
      |WHERE T1.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      |AND   T1.Object="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory1"
      |AND   T2.Predicate="http://schema.org/caption"
      |AND   T3.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre"
      |AND   T4.Predicate="http://schema.org/publisher"
    """.stripMargin


  val q17 =
    """
      |SELECT T1.Subject, T2.Object, T3.Subject
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |INNER JOIN Triples T3 ON T2.Subject=T3.Object
      |INNER JOIN Triples T4 ON T2.Subject=T4.Subject
      |
      |WHERE T1.Predicate="http://xmlns.com/foaf/age"
      |AND   T1.Object="http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup5"
      |AND   T2.Predicate="http://xmlns.com/foaf/familyName"
      |AND   T3.Predicate="http://purl.org/ontology/mo/artist"
      |AND   T4.Predicate="http://schema.org/nationality"
      |AND   T4.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Country1"

    """.stripMargin


  val q18 =
    """
      |SELECT T1.Subject, T2.Object, T3.Object
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
      |INNER JOIN Triples T4 ON T2.Subject=T4.Subject
      |
      |WHERE T1.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      |AND   T1.Object="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory7"
      |AND   T2.Predicate="http://schema.org/description"
      |AND   T3.Predicate="http://schema.org/keywords"
      |AND   T4.Predicate="http://schema.org/language"
      |AND   T4.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Language0"
    """.stripMargin


  val q19 =
    """
      |SELECT T1.Subject,T1.Object,T2.Object
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
      |
      |WHERE T1.Predicate="http://purl.org/ontology/mo/conductor"
      |AND   T2.Predicate= "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      |AND   T3.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre"
      |AND   T3.Object="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre130"
    """.stripMargin


  val q20 =
    """
      |SELECT T1.Subject, T1.Object, T2.Object
      |FROM Triples T1
      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
      |INNER JOIN Triples T3 ON T2.Subject=T3.Object
      |
      |WHERE T1.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      |AND   T2.Predicate="http://schema.org/text"
      |AND   T3.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/User54768"
      |AND   T3.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/likes"
    """.stripMargin








  // Snow-Flake (F)


//  val q4 =
//    """
//      |SELECT T1.Subject, T1.Object, T3.Subject, T5.Object, T6.Object
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
//      |INNER JOIN Triples T3 ON T2.Subject=T3.Object
//      |INNER JOIN Triples T4 ON T3.Subject=T4.Subject
//      |INNER JOIN Triples T5 ON T4.Subject=T5.Subject
//      |INNER JOIN Triples T6 ON T5.Subject=T6.Subject
//      |
//      |WHERE
//      |T1.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
//      |AND T2.Predicate="http://ogp.me/ns#tag"
//      |AND T2.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Topic76"
//      |ANd T3.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre"
//      |AND T4.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
//      |AND T4.Object="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2"
//      |AND T5.Predicate="http://schema.org/keywords"
//      |AND T6.Predicate="http://schema.org/trailer"
//    """.stripMargin
//
//
//  val q5 =
//    """
//      |SELECT T1.Subject, T1.Object, T2.Object, T4.Object, T6.Object, T7.Object, T8.Object
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
//      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
//      |INNER JOIN Triples T4 ON T3.Subject=T4.Subject
//      |INNER JOIN Triples T5 ON T4.Subject=T5.Subject
//      |INNER JOIN Triples T6 ON T5.Subject=T6.Subject
//      |INNER JOIN Triples T7 ON T6.Object=T7.Subject
//      |INNER JOIN Triples T8 ON T7.Subject=T8.Subject
//      |
//      |WHERE
//      |T1.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
//      |AND T2.Predicate="http://ogp.me/ns#title"
//      |AND T3.Predicate="http://schema.org/caption"
//      |AND T4.Predicate="http://schema.org/description"
//      |AND T5.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre"
//      |AND T5.Object="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre100"
//      |AND T6.Predicate="http://xmlns.com/foaf/homepage"
//      |AND T7.Predicate="http://schema.org/url"
//      |AND T8.Predicate ="http://db.uwaterloo.ca/~galuc/wsdbm/hits"
//    """.stripMargin
//
//
//  val q6 =
//
//    """
//      |SELECT T1.Subject, T1.Object, T2.Object, T4.Object, T5.Object,T6.Object
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
//      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
//      |INNER JOIN Triples T4 ON T3.Subject=T4.Object
//      |INNER JOIN Triples T5 ON T4.Subject=T5.Subject
//      |INNER JOIN Triples T6 ON T5.Subject=T6.Object
//      |
//      |WHERE
//      |T1.Predicate="http://schema.org/contentRating"
//      |AND T2.Predicate="http://schema.org/contentSize"
//      |ANd T3.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre"
//      |AND T3.Object="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre70"
//      |AND T4.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor"
//      |AND T5.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/purchaseDate"
//      |AND T6.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/makesPurchase"
//  """.stripMargin
//
//
//  val q7 =
//
//    """
//      |SELECT T1.Subject, T2.Object, T3.Object, T4.Subject, T5.Object, T6.Object, T7.Object, T9.Object
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
//      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
//      |INNER JOIN Triples T4 ON T3.Subject=T4.Object
//      |INNER JOIN Triples T5 ON T3.Subject=T5.Subject
//      |INNER JOIN Triples T6 ON T5.Object=T6.Subject
//      |INNER JOIN Triples T7 ON T3.Subject=T7.Object
//      |INNER JOIN Triples T8 ON T6.Subject=T8.Subject
//      |INNER JOIN Triples T9 ON T8.Subject=T9.Subject
//      |
//      |WHERE
//      |T1.Predicate="http://ogp.me/ns#tag"
//      |AND T1.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Topic231"
//      |AND T2.Predicate="http://schema.org/description"
//      |AND T3.Predicate="http://schema.org/contentSize"
//      |AND T4.Predicate="http://purl.org/goodrelations/includes"
//      |AND T5.Predicate="http://xmlns.com/foaf/homepage"
//      |AND T6.Predicate="http://schema.org/url"
//      |AND T7.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/likes"
//      |AND T8.Predicate ="http://schema.org/language"
//      |AND T8.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Language0"
//      |AND T9.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/hits"
//    """.stripMargin
//
//
//  val q8 =
//    """
//      |SELECT T1.Object, T2.Object, T3.Object, T4.Object, T5.Object, T6.Object
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Object=T2.Subject
//      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
//      |INNER JOIN Triples T4 ON T3.Subject=T4.Subject
//      |INNER JOIN Triples T5 ON T4.Object=T5.Subject
//      |INNER JOIN Triples T6 ON T5.Subject=T6.Subject
//      |
//      |WHERE
//      |T1.Predicate="http://purl.org/goodrelations/offers"
//      |AND T1.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/Retailer113"
//      |AND T2.Predicate="http://purl.org/goodrelations/price"
//      |AND T3.Predicate="http://purl.org/goodrelations/validThrough"
//      |AND T4.Predicate="http://purl.org/goodrelations/includes"
//      |AND T5.Predicate="http://ogp.me/ns#title"
//      |AND T6.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
//    """.stripMargin
//
//
//  // Linear (L)
//
//  val q9 =
//    """
//      |SELECT T1.Subject,T3.Subject,T3.Object
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
//      |INNER JOIN Triples T3 ON T2.Object=T3.Subject
//      |WHERE T1.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/subscribes"
//      |AND   T1.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Website342"
//      |AND   T3.Predicate="http://schema.org/caption"
//    """.stripMargin
//
//
//  val q10 =
//    """
//      |SELECT T1.Subject,T3.Subject
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
//      |INNER JOIN Triples T3 ON T2.Object=T3.Object
//      |WHERE T1.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/likes"
//      |AND   T1.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Product0"
//      |AND   T2.Predicate="http://schema.org/nationality"
//      |AND   T3.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/City39"
//      |AND   T3.Predicate="http://www.geonames.org/ontology#parentCountry"
//    """.stripMargin
//
//
//  val q11 =
//    """
//      |SELECT T1.Subject, T2.Object
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
//      |WHERE T1.Predicate ="http://db.uwaterloo.ca/~galuc/wsdbm/subscribes"
//      |AND T1.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Website221"
//      |AND T2.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/likes"
//    """.stripMargin
//
//
//  val q12 =
//    """
//      |SELECT T1.Subject, T2.Object
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
//      |WHERE T1.Predicate ='http://ogp.me/ns#tag'
//      |AND T1.Object='http://db.uwaterloo.ca/~galuc/wsdbm/Topic248'
//      |AND T2.Predicate='http://schema.org/caption'
//      |""".stripMargin
//
//
//  val q13 =
//    """
//      |SELECT  T1.Subject, T1.Object,  T2.Object
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
//      |INNER JOIN Triples T3 ON T2.Object=T3.Object
//      |WHERE T1.Predicate="http://schema.org/jobTitle"
//      |AND   T2.Predicate="http://schema.org/nationality"
//      |AND   T3.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/City233"
//      |AND   T3.Predicate="http://www.geonames.org/ontology#parentCountry"
//    """.stripMargin
//
//
//  //Star (S)
//
//
//  val q14 =
//    """
//      |SELECT T1.Subject, T1.Object , T3.Object, T4.Object, T5.Object, T6.Object,T7.Object,T8.Object,T9.Object
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Subject=T2.Object
//      |INNER JOIN Triples T3 ON T1.Subject=T3.Subject
//      |INNER JOIN Triples T4 ON T1.Subject=T4.Subject
//      |INNER JOIN Triples T5 ON T1.Subject=T5.Subject
//      |INNER JOIN Triples T6 ON T1.Subject=T6.Subject
//      |INNER JOIN Triples T7 ON T1.Subject=T7.Subject
//      |INNER JOIN Triples T8 ON T1.Subject=T8.Subject
//      |INNER JOIN Triples T9 ON T1.Subject=T9.Subject
//      |
//      |WHERE T1.Predicate="http://purl.org/goodrelations/includes"
//      |AND T2.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/Retailer107"
//      |AND T2.Predicate="http://purl.org/goodrelations/offers"
//      |AND T3.Predicate="http://purl.org/goodrelations/price"
//      |AND T4.Predicate="http://purl.org/goodrelations/serialNumber"
//      |AND T5.Predicate="http://purl.org/goodrelations/validFrom"
//      |AND T6.Predicate="http://purl.org/goodrelations/validThrough"
//      |AND T7.Predicate="http://schema.org/eligibleQuantity"
//      |AND T8.Predicate="http://schema.org/eligibleRegion"
//      |AND T9.Predicate="http://schema.org/priceValidUntil"
//    """.stripMargin
//
//
//  val q15 =
//    """
//      |SELECT T1.Subject, T1.Object, T3.Object
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
//      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
//      |INNER JOIN Triples T4 ON T3.Subject=T4.Subject
//      |WHERE T1.Predicate="http://purl.org/dc/terms/Location"
//      |AND T2.Predicate="http://schema.org/nationality"
//      |AND T2.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Country21"
//      |AND T3.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/gender"
//      |AND T4.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
//      |AND T4.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Role2"
//    """.stripMargin
//
//
//  val q16 =
//    """
//      |SELECT T1.Subject, T2.Object, T3.Object,T4.Object
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
//      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
//      |INNER JOIN Triples T4 ON T3.Subject=T4.Subject
//      |WHERE T1.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
//      |AND   T1.Object="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory4"
//      |AND   T2.Predicate="http://schema.org/caption"
//      |AND   T3.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre"
//      |AND   T4.Predicate="http://schema.org/publisher"
//    """.stripMargin
//
//
//  val q17 =
//    """
//      |SELECT T1.Subject, T2.Object, T3.Subject
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
//      |INNER JOIN Triples T3 ON T2.Subject=T3.Object
//      |INNER JOIN Triples T4 ON T2.Subject=T4.Subject
//      |
//      |WHERE T1.Predicate="http://xmlns.com/foaf/age"
//      |AND   T1.Object="http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup5"
//      |AND   T2.Predicate="http://xmlns.com/foaf/familyName"
//      |AND   T3.Predicate="http://purl.org/ontology/mo/artist"
//      |AND   T4.Predicate="http://schema.org/nationality"
//      |AND   T4.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Country1"
//
//    """.stripMargin
//
//
//  val q18 =
//    """
//      |SELECT T1.Subject, T2.Object, T3.Object
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
//      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
//      |INNER JOIN Triples T4 ON T2.Subject=T4.Subject
//      |
//      |WHERE T1.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
//      |AND   T1.Object="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2"
//      |AND   T2.Predicate="http://schema.org/description"
//      |AND   T3.Predicate="http://schema.org/keywords"
//      |AND   T4.Predicate="http://schema.org/language"
//      |AND   T4.Object="http://db.uwaterloo.ca/~galuc/wsdbm/Language0"
//    """.stripMargin
//
//
//  val q19 =
//    """
//      |SELECT T1.Subject,T1.Object,T2.Object
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
//      |INNER JOIN Triples T3 ON T2.Subject=T3.Subject
//      |
//      |WHERE T1.Predicate="http://purl.org/ontology/mo/conductor"
//      |AND   T2.Predicate= "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
//      |AND   T3.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre"
//      |AND   T3.Object="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre72"
//    """.stripMargin
//
//
//  val q20 =
//    """
//      |SELECT T1.Subject, T1.Object, T2.Object
//      |FROM Triples T1
//      |INNER JOIN Triples T2 ON T1.Subject=T2.Subject
//      |INNER JOIN Triples T3 ON T2.Subject=T3.Object
//      |
//      |WHERE T1.Predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
//      |AND   T2.Predicate="http://schema.org/text"
//      |AND   T3.Subject="http://db.uwaterloo.ca/~galuc/wsdbm/User5641"
//      |AND   T3.Predicate="http://db.uwaterloo.ca/~galuc/wsdbm/likes"
//    """.stripMargin
}
