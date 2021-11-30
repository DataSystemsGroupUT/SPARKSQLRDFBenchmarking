package ee.ut.cs.bigdata.watdiv

import java.io.{File, FileOutputStream}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel._

object CreateWPTTable {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench Create WPT")
      .getOrCreate()

    println("Spark Session is created")

    import spark.implicits._
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    val ds = args(0) // value = {"100K", "100M", "500M, or "1B"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds"

    //read tables from HDFS
    val RDFDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s"$path/ST/VHDFS/CSV/ST$ds.csv").toDF()
    RDFDF.createOrReplaceTempView("triples")
    println("Table is read")

    val wptTable = spark.sql(
      """
        |select DISTINCT Pred1.Subject, Pred1.type, Pred2.expires, Pred3.producer,
        |Pred4.purchaseDate, Pred5.aggregateRating, Pred6.contactPoint, Pred7.subscribes, Pred8.employee, Pred9.conductor, Pred10.language, Pred11.release, Pred12.validFrom, Pred13.birthDate, Pred14.name, Pred15.Location, Pred16.likes, Pred17.trailer, Pred18.performed_in, Pred19.faxNumber, Pred20.caption, Pred21.paymentAccepted, Pred22.keywords, Pred23.tag, Pred24.author, Pred25.text, Pred26.performer, Pred27.nationality, Pred28.duration, Pred29.hasReview, Pred30.numberOfPages, Pred31.openingHours, Pred32.includes, Pred33.gender, Pred34.rating, Pred35.printPage, Pred36.reviewer, Pred37.eligibleRegion, Pred38.hits, Pred39.priceValidUntil, Pred40.contentRating, Pred41.telephone, Pred42.rev_title, Pred43.age, Pred44.award, Pred45.friendOf, Pred46.title, Pred47.printEdition, Pred48.homepage, Pred49.parentCountry, Pred50.familyName, Pred51.legalName, Pred52.publisher, Pred53.artist, Pred54.opus, Pred55.printColumn, Pred56.offers, Pred57.datePublished, Pred58.movement, Pred59.description, Pred60.validThrough, Pred61.jobTitle, Pred62.url, Pred63.price, Pred64.producer, Pred65.purchaseFor, Pred66.composer, Pred67.totalVotes, Pred68.director, Pred69.description, Pred70.actor, Pred71.email, Pred72.contentSize, Pred73.givenName, Pred74.makesPurchase, Pred75.serialNumber, Pred76.hasGenr, Pred77.follows, Pred78.wordCount, Pred79.userId,
        |Pred80.printSection, Pred81.record_number, Pred82.text, Pred83.eligibleQuantity, Pred84.editor, Pred85.bookEdition, Pred86.isbn
        |FROM(
        |select T1.Subject, T1.Object as type
        |FROM triples T1
        |where T1.Predicate="<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
        |)Pred1
        |
        |
        |LEFT JOIN
        |(
        |select T2.Subject, T2.Object as expires
        |FROM triples T2
        |where T2.Predicate="<http://schema.org/expires>"
        |)Pred2
        |ON Pred1.Subject=Pred2.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T3.Subject, T3.Object as producer
        |FROM triples T3
        |where T3.Predicate="<http://schema.org/producer>"
        |)Pred3
        |ON Pred1.Subject=Pred3.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T4.Subject, T4.Object as purchaseDate
        |FROM triples T4
        |where T4.Predicate="<http://db.uwaterloo.ca/~galuc/wsdbm/purchaseDate>"
        |)Pred4
        |ON Pred1.Subject=Pred4.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T5.Subject, T5.Object as aggregateRating
        |FROM triples T5
        |where T5.Predicate="<http://schema.org/aggregateRating>"
        |)Pred5
        |ON Pred1.Subject=Pred5.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T6.Subject, T6.Object as contactPoint
        |FROM triples T6
        |where T6.Predicate="<http://schema.org/contactPoint>"
        |)Pred6
        |ON Pred1.Subject=Pred6.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T7.Subject, T7.Object as subscribes
        |FROM triples T7
        |where T7.Predicate="<http://db.uwaterloo.ca/~galuc/wsdbm/subscribes>"
        |)Pred7
        |ON Pred1.Subject=Pred7.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T8.Subject, T8.Object as employee
        |FROM triples T8
        |where T8.Predicate="<http://schema.org/employee>"
        |)Pred8
        |ON Pred1.Subject=Pred8.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T9.Subject, T9.Object as conductor
        |FROM triples T9
        |where T9.Predicate="<http://purl.org/ontology/mo/conductor>"
        |)Pred9
        |ON Pred1.Subject=Pred9.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T10.Subject, T10.Object as language
        |FROM triples T10
        |where T10.Predicate="<http://schema.org/language>"
        )Pred10
        |ON Pred1.Subject=Pred10.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T11.Subject, T11.Object as release
        |FROM triples T11
        |where T11.Predicate="<http://purl.org/ontology/mo/release>"
        )Pred11
        |ON Pred1.Subject=Pred11.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T12.Subject, T12.Object as validFrom
        |FROM triples T12
        |where T12.Predicate="<http://purl.org/goodrelations/validFrom>"
        )Pred12
        |ON Pred1.Subject=Pred12.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T13.Subject, T13.Object as birthDate
        |FROM triples T13
        |where T13.Predicate="<http://schema.org/birthDate>"
        )Pred13
        |ON Pred1.Subject=Pred13.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T14.Subject, T14.Object as name
        |FROM triples T14
        |where T14.Predicate="<http://purl.org/goodrelations/name>"
        )Pred14
        |ON Pred1.Subject=Pred14.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T15.Subject, T15.Object as Location
        |FROM triples T15
        |where T15.Predicate="<http://purl.org/dc/terms/Location>"
        )Pred15
        |ON Pred1.Subject=Pred15.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T16.Subject, T16.Object as likes
        |FROM triples T16
        |where T16.Predicate="<http://db.uwaterloo.ca/~galuc/wsdbm/likes>"
        )Pred16
        |ON Pred1.Subject=Pred16.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T17.Subject, T17.Object as trailer
        |FROM triples T17
        |where T17.Predicate="<http://schema.org/trailer>"
        )Pred17
        |ON Pred1.Subject=Pred17.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T18.Subject, T18.Object as performed_in
        |FROM triples T18
        |where T18.Predicate="<http://purl.org/ontology/mo/performed_in>"
        )Pred18
        |ON Pred1.Subject=Pred18.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T19.Subject, T19.Object as faxNumber
        |FROM triples T19
        |where T19.Predicate="<http://schema.org/faxNumber>"
        )Pred19
        |ON Pred1.Subject=Pred19.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T20.Subject, T20.Object as caption
        |FROM triples T20
        |where T20.Predicate="<http://schema.org/caption>"
        )Pred20
        |ON Pred1.Subject=Pred20.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T21.Subject, T21.Object as paymentAccepted
        |FROM triples T21
        |where T21.Predicate="<http://schema.org/paymentAccepted>"
        )Pred21
        |ON Pred1.Subject=Pred21.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T22.Subject, T22.Object as keywords
        |FROM triples T22
        |where T22.Predicate="<http://schema.org/keywords>"
        )Pred22
        |ON Pred1.Subject=Pred22.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T23.Subject, T23.Object as tag
        |FROM triples T23
        |where T23.Predicate="<http://ogp.me/ns#tag>"
        )Pred23
        |ON Pred1.Subject=Pred23.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T24.Subject, T24.Object as author
        |FROM triples T24
        |where T24.Predicate="<http://schema.org/author>"
        )Pred24
        |ON Pred1.Subject=Pred24.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T25.Subject, T25.Object as text
        |FROM triples T25
        |where T25.Predicate="<http://schema.org/text>"
        )Pred25
        |ON Pred1.Subject=Pred25.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T26.Subject, T26.Object as performer
        |FROM triples T26
        |where T26.Predicate="<http://purl.org/ontology/mo/performer>"
        )Pred26
        |ON Pred1.Subject=Pred26.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T27.Subject, T27.Object as nationality
        |FROM triples T27
        |where T27.Predicate="<http://schema.org/nationality>"
        )Pred27
        |ON Pred1.Subject=Pred27.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T28.Subject, T28.Object as duration
        |FROM triples T28
        |where T28.Predicate="<http://schema.org/duration>"
        )Pred28
        |ON Pred1.Subject=Pred28.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T29.Subject, T29.Object as hasReview
        |FROM triples T29
        |where T29.Predicate="<http://purl.org/stuff/rev#hasReview>"
        )Pred29
        |ON Pred1.Subject=Pred29.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T30.Subject, T30.Object as numberOfPages
        |FROM triples T30
        |where T30.Predicate="<http://schema.org/numberOfPages>"
        )Pred30
        |ON Pred1.Subject=Pred30.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T31.Subject, T31.Object as openingHours
        |FROM triples T31
        |where T31.Predicate="<http://schema.org/openingHours>"
        )Pred31
        |ON Pred1.Subject=Pred31.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T32.Subject, T32.Object as includes
        |FROM triples T32
        |where T32.Predicate="<http://purl.org/goodrelations/includes>"
        )Pred32
        |ON Pred1.Subject=Pred32.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T33.Subject, T33.Object as gender
        |FROM triples T33
        |where T33.Predicate="<http://db.uwaterloo.ca/~galuc/wsdbm/gender>"
        )Pred33
        |ON Pred1.Subject=Pred33.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T34.Subject, T34.Object as rating
        |FROM triples T34
        |where T34.Predicate="<http://purl.org/stuff/rev#rating>"
        )Pred34
        |ON Pred1.Subject=Pred34.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T35.Subject, T35.Object as printPage
        |FROM triples T35
        |where T35.Predicate="<http://schema.org/printPage>"
        )Pred35
        |ON Pred1.Subject=Pred35.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T36.Subject, T36.Object as reviewer
        |FROM triples T36
        |where T36.Predicate="<http://purl.org/stuff/rev#reviewer>"
        )Pred36
        |ON Pred1.Subject=Pred36.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T37.Subject, T37.Object as eligibleRegion
        |FROM triples T37
        |where T37.Predicate="<http://schema.org/eligibleRegion>"
        )Pred37
        |ON Pred1.Subject=Pred37.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T38.Subject, T38.Object as hits
        |FROM triples T38
        |where T38.Predicate="<http://db.uwaterloo.ca/~galuc/wsdbm/hits>"
        )Pred38
        |ON Pred1.Subject=Pred38.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T39.Subject, T39.Object as priceValidUntil
        |FROM triples T39
        |where T39.Predicate="<http://schema.org/priceValidUntil>"
        )Pred39
        |ON Pred1.Subject=Pred39.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T40.Subject, T40.Object as contentRating
        |FROM triples T40
        |where T40.Predicate="<http://schema.org/contentRating>"
        )Pred40
        |ON Pred1.Subject=Pred40.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T41.Subject, T41.Object as telephone
        |FROM triples T41
        |where T41.Predicate="<http://schema.org/telephone>"
        )Pred41
        |ON Pred1.Subject=Pred41.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T42.Subject, T42.Object as rev_title
        |FROM triples T42
        |where T42.Predicate="<http://purl.org/stuff/rev#title>"
        )Pred42
        |ON Pred1.Subject=Pred42.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T43.Subject, T43.Object as age
        |FROM triples T43
        |where T43.Predicate="<http://xmlns.com/foaf/age>"
        )Pred43
        |ON Pred1.Subject=Pred43.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T44.Subject, T44.Object as award
        |FROM triples T44
        |where T44.Predicate="<http://schema.org/award>"
        )Pred44
        |ON Pred1.Subject=Pred44.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T45.Subject, T45.Object as friendOf
        |FROM triples T45
        |where T45.Predicate="<http://db.uwaterloo.ca/~galuc/wsdbm/friendOf>"
        )Pred45
        |ON Pred1.Subject=Pred45.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T46.Subject, T46.Object as title
        |FROM triples T46
        |where T46.Predicate="<http://ogp.me/ns#title>"
        )Pred46
        |ON Pred1.Subject=Pred46.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T47.Subject, T47.Object as printEdition
        |FROM triples T47
        |where T47.Predicate="<http://schema.org/printEdition>"
        )Pred47
        |ON Pred1.Subject=Pred47.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T48.Subject, T48.Object as homepage
        |FROM triples T48
        |where T48.Predicate="<http://xmlns.com/foaf/homepage>"
        )Pred48
        |ON Pred1.Subject=Pred48.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T49.Subject, T49.Object as parentCountry
        |FROM triples T49
        |where T49.Predicate="<http://www.geonames.org/ontology#parentCountry>"
        )Pred49
        |ON Pred1.Subject=Pred49.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T50.Subject, T50.Object as familyName
        |FROM triples T50
        |where T50.Predicate="<http://xmlns.com/foaf/familyName>"
        )Pred50
        |ON Pred1.Subject=Pred50.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T51.Subject, T51.Object as legalName
        |FROM triples T51
        |where T51.Predicate="<http://schema.org/legalName>"
        )Pred51
        |ON Pred1.Subject=Pred51.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T52.Subject, T52.Object as publisher
        |FROM triples T52
        |where T52.Predicate="<http://schema.org/publisher>"
        )Pred52
        |ON Pred1.Subject=Pred52.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T53.Subject, T53.Object as artist
        |FROM triples T53
        |where T53.Predicate="<http://purl.org/ontology/mo/artist>"
        )Pred53
        |ON Pred1.Subject=Pred53.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T54.Subject, T54.Object as opus
        |FROM triples T54
        |where T54.Predicate="<http://purl.org/ontology/mo/opus>"
        )Pred54
        |ON Pred1.Subject=Pred54.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T55.Subject, T55.Object as printColumn
        |FROM triples T55
        |where T55.Predicate="<http://schema.org/printColumn>"
        )Pred55
        |ON Pred1.Subject=Pred55.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T56.Subject, T56.Object as offers
        |FROM triples T56
        |where T56.Predicate="<http://purl.org/goodrelations/offers>"
        )Pred56
        |ON Pred1.Subject=Pred56.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T57.Subject, T57.Object as datePublished
        |FROM triples T57
        |where T57.Predicate="<http://schema.org/datePublished>"
        )Pred57
        |ON Pred1.Subject=Pred57.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T58.Subject, T58.Object as movement
        |FROM triples T58
        |where T58.Predicate="<http://purl.org/ontology/mo/movement>"
        )Pred58
        |ON Pred1.Subject=Pred58.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T59.Subject, T59.Object as description
        |FROM triples T59
        |where T59.Predicate="<http://purl.org/goodrelations/description>"
        )Pred59
        |ON Pred1.Subject=Pred59.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T60.Subject, T60.Object as validThrough
        |FROM triples T60
        |where T60.Predicate="<http://purl.org/goodrelations/validThrough>"
        )Pred60
        |ON Pred1.Subject=Pred60.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T61.Subject, T61.Object as jobTitle
        |FROM triples T61
        |where T61.Predicate="<http://schema.org/jobTitle>"
        )Pred61
        |ON Pred1.Subject=Pred61.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T62.Subject, T62.Object as url
        |FROM triples T62
        |where T62.Predicate="<http://schema.org/url>"
        )Pred62
        |ON Pred1.Subject=Pred62.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T63.Subject, T63.Object as price
        |FROM triples T63
        |where T63.Predicate="<http://purl.org/goodrelations/price>"
        )Pred63
        |ON Pred1.Subject=Pred63.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T64.Subject, T64.Object as producer
        |FROM triples T64
        |where T64.Predicate="<http://purl.org/ontology/mo/producer>"
        )Pred64
        |ON Pred1.Subject=Pred64.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T65.Subject, T65.Object as purchaseFor
        |FROM triples T65
        |where T65.Predicate="<http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor>"
        )Pred65
        |ON Pred1.Subject=Pred65.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T66.Subject, T66.Object as composer
        |FROM triples T66
        |where T66.Predicate="<http://db.uwaterloo.ca/~galuc/wsdbm/composer>"
        )Pred66
        |ON Pred1.Subject=Pred66.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T67.Subject, T67.Object as totalVotes
        |FROM triples T67
        |where T67.Predicate="<http://purl.org/stuff/rev#totalVotes>"
        )Pred67
        |ON Pred1.Subject=Pred67.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T68.Subject, T68.Object as director
        |FROM triples T68
        |where T68.Predicate="<http://schema.org/director>"
        )Pred68
        |ON Pred1.Subject=Pred68.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T69.Subject, T69.Object as description
        |FROM triples T69
        |where T69.Predicate="<http://schema.org/description>"
        )Pred69
        |ON Pred1.Subject=Pred69.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T70.Subject, T70.Object as actor
        |FROM triples T70
        |where T70.Predicate="<http://schema.org/actor>"
        )Pred70
        |ON Pred1.Subject=Pred70.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T71.Subject, T71.Object as email
        |FROM triples T71
        |where T71.Predicate="<http://schema.org/email>"
        )Pred71
        |ON Pred1.Subject=Pred71.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T72.Subject, T72.Object as contentSize
        |FROM triples T72
        |where T72.Predicate="<http://schema.org/contentSize>"
        )Pred72
        |ON Pred1.Subject=Pred72.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T73.Subject, T73.Object as givenName
        |FROM triples T73
        |where T73.Predicate="<http://xmlns.com/foaf/givenName>"
        )Pred73
        |ON Pred1.Subject=Pred73.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T74.Subject, T74.Object as makesPurchase
        |FROM triples T74
        |where T74.Predicate="<http://db.uwaterloo.ca/~galuc/wsdbm/makesPurchase>"
        )Pred74
        |ON Pred1.Subject=Pred74.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T75.Subject, T75.Object as serialNumber
        |FROM triples T75
        |where T75.Predicate="<http://purl.org/goodrelations/serialNumber>"
        )Pred75
        |ON Pred1.Subject=Pred75.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T76.Subject, T76.Object as hasGenr
        |FROM triples T76
        |where T76.Predicate="<http://db.uwaterloo.ca/~galuc/wsdbm/hasGenr"
        )Pred76
        |ON Pred1.Subject=Pred76.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T77.Subject, T77.Object as follows
        |FROM triples T77
        |where T77.Predicate="<http://db.uwaterloo.ca/~galuc/wsdbm/follows"
        )Pred77
        |ON Pred1.Subject=Pred77.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T78.Subject, T78.Object as wordCount
        |FROM triples T78
        |where T78.Predicate="<http://schema.org/wordCount>"
        )Pred78
        |ON Pred1.Subject=Pred78.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T79.Subject, T79.Object as userId
        |FROM triples T79
        |where T79.Predicate="<http://db.uwaterloo.ca/~galuc/wsdbm/userId>"
        )Pred79
        |ON Pred1.Subject=Pred79.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T80.Subject, T80.Object as printSection
        |FROM triples T80
        |where T80.Predicate="<http://schema.org/printSection>"
        )Pred80
        |ON Pred1.Subject=Pred80.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T81.Subject, T81.Object as record_number
        |FROM triples T81
        |where T81.Predicate="<http://purl.org/ontology/mo/record_number>"
        )Pred81
        |ON Pred1.Subject=Pred81.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T82.Subject, T82.Object as text
        |FROM triples T82
        |where T82.Predicate="<http://purl.org/stuff/rev#text>"
        )Pred82
        |ON Pred1.Subject=Pred82.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T83.Subject, T83.Object as eligibleQuantity
        |FROM triples T83
        |where T83.Predicate="<http://schema.org/eligibleQuantity>"
        )Pred83
        |ON Pred1.Subject=Pred83.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T84.Subject, T84.Object as editor
        |FROM triples T84
        |where T84.Predicate="<http://schema.org/editor>"
        )Pred84
        |ON Pred1.Subject=Pred84.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T85.Subject, T85.Object as bookEdition
        |FROM triples T85
        |where T85.Predicate="<http://schema.org/bookEdition>"
        )Pred85
        |ON Pred1.Subject=Pred85.Subject
        |
        |
        |LEFT JOIN
        |(
        |select T86.Subject, T86.Object as isbn
        |FROM triples T86
        |where T86.Predicate="<http://schema.org/isbn>"
        )Pred86
        |ON Pred1.Subject=Pred86.Subject
        |
      """.stripMargin)

    wptTable.coalesce(1).write.format("csv").option("header", "true").save(s"$path/WPT/VHDFS/CSV/" + "WidePropertyTable.csv")
    println("Saved CSV WPT")
    wptTable.coalesce(1).write.parquet(s"$path/WPT/VHDFS/Parquet/" + "WidePropertyTable.parquet")
    println("Saved Parquet WPT")
    wptTable.coalesce(1).write.orc(s"$path/WPT/VHDFS/ORC/" + "WidePropertyTable.orc")
    println("Saved ORC WPT")
    wptTable.coalesce(1).write.format("avro").save(s"$path/WPT/VHDFS/Avro/" + "WidePropertyTable.avro")
    println("Saved Avro WPT")

  }
}
