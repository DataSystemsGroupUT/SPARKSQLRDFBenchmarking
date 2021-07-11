package ee.ut.cs.bigdata.watdiv.queries

class VTQueries {

  //Complex

  val q1 =
    """
      |SELECT tab0.v1 AS v1 , tab0.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2
      |FROM    (SELECT object AS v1 , subject AS v0  FROM caption) tab0
      |JOIN    (SELECT subject AS v0 , object AS v2 FROM text) tab1
      |ON(tab0.v0=tab1.v0)
      |JOIN    (SELECT subject AS v0 , object AS v3 FROM contentRating) tab2
      |ON(tab1.v0=tab2.v0)
      |JOIN    (SELECT subject AS v0 , object AS v4 FROM hasReview) tab3
      |ON(tab2.v0=tab3.v0)
      |JOIN    (SELECT object AS v5 , subject AS v4
      |FROM revTitle) tab4
      |ON(tab3.v4=tab4.v4)
      |JOIN    (SELECT object AS v6 , subject AS v4 FROM reviewer) tab5
      |ON(tab4.v4=tab5.v4)
      |JOIN    (SELECT subject AS v7 , object AS v6 FROM actor) tab6
      |ON(tab5.v6=tab6.v6)
      |JOIN    (SELECT subject AS v7 , object AS v8 FROM language) tab7
      | ON(tab6.v7=tab7.v7)
  """.stripMargin


  val q2 =
    """
      |SELECT tab1.v0 AS v0 , tab3.v3 AS v3, tab6.v4 AS v4 , tab8.v8 AS v8
      |FROM    (SELECT subject AS v2 FROM eligibleRegion WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Country3') tab2
      |JOIN    (SELECT object AS v3 , subject AS v2 FROM includes) tab3
      |ON(tab2.v2=tab3.v2)
      |JOIN    (SELECT subject AS v0 , object AS v2 FROM offers ) tab1
      |ON(tab3.v2=tab1.v2)
      |JOIN    (SELECT object AS v1 , subject AS v0
      |FROM legalName) tab0
      |ON(tab1.v0=tab0.v0)
      |JOIN    (SELECT subject AS v3 , object AS v8 FROM hasReview) tab8
      |ON(tab3.v3=tab8.v3)
      |JOIN    (SELECT object AS v9 , subject AS v8
      |FROM totalVotes) tab9
      |ON(tab8.v8=tab9.v8)
      |JOIN    (SELECT subject AS v7 , object AS v3 FROM purchaseFor  ) tab7
      |ON(tab8.v3=tab7.v3)
      |JOIN    (SELECT object AS v7 , subject AS v4 FROM makesPurchase) tab6
      |ON(tab7.v7=tab6.v7)
      |JOIN    (SELECT object AS v5 , subject AS v4
      |FROM jobTitle) tab4
      |ON(tab6.v4=tab4.v4)
      |JOIN    (SELECT object AS v6 , subject AS v4
      |FROM homepage) tab5
      |ON(tab4.v4=tab5.v4)
""".stripMargin


  val q3 =
    """
      |SELECT tab2.v0 AS v0
      |FROM    (SELECT subject AS v0 , object AS v3 FROM Location) tab2
      |JOIN    (SELECT subject AS v0 , object AS v4 FROM age) tab3
      |ON(tab2.v0=tab3.v0)
      |JOIN    (SELECT subject AS v0 , object AS v5 FROM gender) tab4
      |ON(tab3.v0=tab4.v0)
      |JOIN    (SELECT subject AS v0 , object AS v6
      |FROM givenName) tab5
      |ON(tab4.v0=tab5.v0)
      |JOIN    (SELECT object AS v1 , subject AS v0
      |FROM likes) tab0
      |ON(tab5.v0=tab0.v0)
      |JOIN    (SELECT subject AS v0 , object AS v2
      |FROM friendOf) tab1
      |ON(tab0.v0=tab1.v0)
  """.stripMargin



  // Snow-Flake (F)


  val q4 =
    """
      |SELECT tab4.v0 AS v0 , tab3.v5 AS v5 , tab2.v4 AS v4 , tab5.v3 AS v3 , tab1.v2 AS v2
      | FROM    (SELECT subject AS v3  FROM type WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2'	) tab5
      | JOIN    (SELECT object AS v4 , subject AS v3 FROM trailer	) tab2
      | ON(tab5.v3=tab2.v3)
      | JOIN    (SELECT object AS v5 , subject AS v3	 FROM keywords	) tab3
      | ON(tab2.v3=tab3.v3)
      | JOIN    (SELECT object AS v0 , subject AS v3	 FROM hasGenre	) tab4
      | ON(tab3.v3=tab4.v3)
      | JOIN    (SELECT subject AS v0 FROM tag WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Topic8'	) tab0
      | ON(tab4.v0=tab0.v0) JOIN    (SELECT subject AS v0 , object AS v2	 FROM type	) tab1
      | ON(tab0.v0=tab1.v0)
    """.stripMargin


  val q5 =
    """
      |SELECT tab0.v1 AS v1 , tab7.v0 AS v0 , tab4.v5 AS v5 , tab6.v7 AS v7 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab1.v2 AS v2
      |FROM    (SELECT subject AS v0 FROM hasGenre WHERE object = "http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre117") tab7
      | JOIN    (SELECT subject AS v0 , object AS v4	 FROM caption	) tab3
      | ON(tab7.v0=tab3.v0)
      | JOIN    (SELECT object AS v1 , subject AS v0 FROM homepage	) tab0
      | ON(tab3.v0=tab0.v0)
      | JOIN    (SELECT subject AS v1 , object AS v6	 FROM url	) tab5
      | ON(tab0.v1=tab5.v1)
      | JOIN    (SELECT subject AS v1 , object AS v7  FROM hits	) tab6
      | ON(tab5.v1=tab6.v1)
      | JOIN    (SELECT subject AS v0 , object AS v5
      | FROM description	) tab4
      | ON(tab0.v0=tab4.v0)
      | JOIN    (SELECT subject AS v0 , object AS v2 FROM title ) tab1
      | ON(tab4.v0=tab1.v0)
      | JOIN    (SELECT subject AS v0 , object AS v3 FROM type) tab2
      | ON(tab1.v0=tab2.v0)
    """.stripMargin


  val q6 =
    """
      |SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab5.v5 AS v5 , tab3.v4 AS v4 , tab4.v6 AS v6 , tab1.v2 AS v2
      |FROM    (SELECT subject AS v0
      |FROM hasGenre
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre111'
      |) tab2
      |JOIN    (SELECT subject AS v0 , object AS v2
      |FROM contentSize
      |) tab1
      |ON(tab2.v0=tab1.v0)
      |JOIN    (SELECT object AS v1 , subject AS v0
      |FROM contentRating
      |) tab0
      |ON(tab1.v0=tab0.v0)
      |JOIN    (SELECT object AS v0 , subject AS v5
      |FROM purchaseFor
      |) tab5
      |ON(tab0.v0=tab5.v0)
      |JOIN    (SELECT subject AS v5 , object AS v6
      |FROM purchaseDate
      |) tab4
      |ON(tab5.v5=tab4.v5)
      |JOIN    (SELECT object AS v5 , subject AS v4
      |FROM makesPurchase
      |) tab3
      |ON(tab4.v5=tab3.v5)
      """.stripMargin


  val q7 =
    """
      |SELECT tab7.v1 AS v1 , tab0.v0 AS v0 , tab8.v7 AS v7 , tab5.v5 AS v5 , tab3.v4 AS v4 , tab6.v6 AS v6 , tab1.v2 AS v2 , tab4.v8 AS v8
      |FROM    (SELECT subject AS v1	 FROM language WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Language0'	) tab7
      |JOIN    (SELECT subject AS v1 , object AS v5	 FROM url) tab5  ON(tab7.v1=tab5.v1)
      |JOIN    (SELECT subject AS v1 , object AS v6  FROM hits ) tab6
      |ON(tab5.v1=tab6.v1)
      |JOIN    (SELECT object AS v1 , subject AS v0 FROM homepage ) tab0
      |ON(tab6.v1=tab0.v1)
      |JOIN    (SELECT subject AS v0 FROM tag WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Topic122'	) tab2
      |ON(tab0.v0=tab2.v0) JOIN    (SELECT subject AS v0 , object AS v8 FROM contentSize	) tab4
      |ON(tab2.v0=tab4.v0) JOIN    (SELECT subject AS v0 , object AS v4	 FROM description	) tab3
      |ON(tab4.v0=tab3.v0) JOIN    (SELECT object AS v0 , subject AS v2	 FROM includes	) tab1
      |ON(tab3.v0=tab1.v0) JOIN    (SELECT object AS v0 , subject AS v7	 FROM likes	) tab8
      |ON(tab1.v0=tab8.v0)
    """.stripMargin

  val q8 =
    """
      |SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3
      |FROM    (SELECT object AS v0 FROM offers WHERE subject = 'http://db.uwaterloo.ca/~galuc/wsdbm/Retailer9885' ) tab1
      |JOIN    (SELECT subject AS v0 , object AS v4 FROM validThrough	) tab3
      |ON(tab1.v0=tab3.v0)
      |JOIN    (SELECT object AS v1 , subject AS v0 FROM includes ) tab0
      |ON(tab3.v0=tab0.v0)
      |JOIN    (SELECT subject AS v1 , object AS v5 FROM title) tab4
      |ON(tab0.v1=tab4.v1)
      |JOIN    (SELECT subject AS v1 , object AS v6 FROM type	) tab5
      |ON(tab4.v1=tab5.v1)
      |JOIN    (SELECT subject AS v0 , object AS v3 FROM price	) tab2
      |ON(tab0.v0=tab2.v0)
    """.stripMargin


  // Linear (L)


  val q9 =
    """
      |SELECT tab0.v0 AS v0 , tab1.v3 AS v3 , tab2.v2 AS v2
      |FROM    (SELECT subject AS v0 FROM subscribes
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Website7355') tab0
      |JOIN    (SELECT subject AS v0 , object AS v2 FROM likes ) tab2 ON(tab0.v0=tab2.v0)
      |JOIN    (SELECT object AS v3 , subject AS v2 FROM caption) tab1 ON(tab2.v2=tab1.v2)
    """.stripMargin


  val q10 =
    """
      |SELECT tab0.v1 AS v1 , tab2.v2 AS v2
      |FROM    (SELECT object AS v1 FROM parentCountry WHERE subject = 'http://db.uwaterloo.ca/~galuc/wsdbm/City70') tab0
      |JOIN    (SELECT object AS v1 , subject AS v2 FROM nationality) tab2
      |ON(tab0.v1=tab2.v1)
      |JOIN    (SELECT subject AS v2 FROM likes WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Product0') tab1
      |ON(tab2.v2=tab1.v2)
    """.stripMargin


  val q11 =
    """
      |SELECT tab1.v1 AS v1 , tab0.v0 AS v0
      |FROM
      |(SELECT subject AS v0 FROM subscribes
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Website43164') tab0  JOIN
      |(SELECT object AS v1 , subject AS v0 FROM likes) tab1 ON(tab0.v0=tab1.v0)
    """.stripMargin


  val q12 =
    """
      |SELECT tab0.v0 AS v0 , tab1.v2 AS v2
      |FROM   (SELECT subject AS v0  FROM tag  WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Topic142') tab0
      | JOIN  (SELECT subject AS v0 , object AS v2 FROM caption) tab1 ON(tab0.v0=tab1.v0)
    """.stripMargin


  val q13 =
    """
      |SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab1.v3 AS v3
      |FROM   (SELECT object AS v3 FROM parentCountry
      |WHERE subject = 'http://db.uwaterloo.ca/~galuc/wsdbm/City40') tab1
      |JOIN   (SELECT subject AS v0 , object AS v3
      |FROM nationality) tab2
      |ON(tab1.v3=tab2.v3)
      |JOIN   (SELECT object AS v1 , subject AS v0  FROM jobTitle) tab0
      |ON(tab2.v0=tab0.v0)
    """.stripMargin


  //Star (S)


  val q14 =
    """
      |SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab3.v4 AS v4 , tab5.v6 AS v6 , tab2.v3 AS v3 , tab8.v9 AS v9 , tab7.v8 AS v8
      |FROM    (SELECT object AS v0
      |FROM offers
      |WHERE subject = 'http://db.uwaterloo.ca/~galuc/wsdbm/Retailer8535'
      |) tab1
      |JOIN    (SELECT subject AS v0 , object AS v9
      |FROM priceValidUntil
      |) tab8
      |ON(tab1.v0=tab8.v0)
      |JOIN    (SELECT subject AS v0 , object AS v5
      |FROM validFrom ) tab4
      |ON(tab8.v0=tab4.v0)
      |JOIN    (SELECT subject AS v0 , object AS v6
      |FROM validThrough
      |) tab5
      |ON(tab4.v0=tab5.v0)
      |JOIN    (SELECT object AS v1 , subject AS v0
      |FROM includes
      |) tab0
      |ON(tab5.v0=tab0.v0)
      |JOIN    (SELECT subject AS v0 , object AS v4
      |FROM serialNumber ) tab3
      |ON(tab0.v0=tab3.v0)
      |JOIN    (SELECT subject AS v0 , object AS v7
      |FROM eligibleQuantity
      |) tab6
      |ON(tab3.v0=tab6.v0)
      |JOIN    (SELECT subject AS v0 , object AS v8
      |FROM eligibleRegion
      |) tab7
      |ON(tab6.v0=tab7.v0)
      |JOIN    (SELECT subject AS v0 , object AS v3
      |FROM price
      |) tab2
      |ON(tab7.v0=tab2.v0)
    """.stripMargin


  val q15 =
    """
      |SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab2.v3 AS v3
      |FROM    (SELECT subject AS v0
      |FROM nationality
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Country4'
      |) tab1
      |JOIN    (SELECT subject AS v0
      |FROM type
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Role2'
      |) tab3
      |ON(tab1.v0=tab3.v0)
      |JOIN    (SELECT object AS v1 , subject AS v0
      |FROM Location
      |) tab0
      |ON(tab3.v0=tab0.v0)
      |JOIN    (SELECT subject AS v0 , object AS v3
      |FROM gender
      |) tab2
      |ON(tab0.v0=tab2.v0)
    """.stripMargin


  val q16 =
    """
      |SELECT tab0.v0 AS v0 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2
      |FROM    (SELECT subject AS v0
      |FROM type
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory4'
      |) tab0
      |JOIN    (SELECT subject AS v0 , object AS v4
      |FROM publisher
      |) tab3
      |ON(tab0.v0=tab3.v0)
      |JOIN    (SELECT subject AS v0 , object AS v2
      |FROM caption
      |) tab1
      |ON(tab3.v0=tab1.v0)
      |JOIN    (SELECT subject AS v0 , object AS v3
      |FROM hasGenre
      |) tab2
      |ON(tab1.v0=tab2.v0)
    """.stripMargin


  val q17 =
    """
      |SELECT tab3.v0 AS v0 , tab2.v3 AS v3 , tab1.v2 AS v2
      |FROM    (SELECT subject AS v0
      |FROM nationality
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Country1'
      |) tab3
      |JOIN    (SELECT subject AS v0
      |FROM age
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup5'
      |) tab0
      |ON(tab3.v0=tab0.v0)
      |JOIN    (SELECT object AS v0 , subject AS v3
      |FROM artist
      |) tab2
      |ON(tab0.v0=tab2.v0)
      |JOIN    (SELECT subject AS v0 , object AS v2
      |FROM familyName
      |) tab1
      |ON(tab2.v0=tab1.v0)
    """.stripMargin


  val q18 =
    """
      |SELECT tab3.v0 AS v0 , tab2.v3 AS v3 , tab1.v2 AS v2
 FROM    (SELECT subject AS v0
	 FROM language

	 WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/Language0'
	) tab3
 JOIN    (SELECT subject AS v0
	 FROM type
	 WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory3'
	) tab0
 ON(tab3.v0=tab0.v0)
 JOIN    (SELECT subject AS v0 , object AS v3
	 FROM keywords

	) tab2
 ON(tab0.v0=tab2.v0)
 JOIN    (SELECT subject AS v0 , object AS v2
	 FROM description

	) tab1
 ON(tab2.v0=tab1.v0)
    """.stripMargin


  val q19 =
    """
      |SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab1.v2 AS v2
      |FROM    (SELECT subject AS v0
      |FROM hasGenre
      |WHERE object = 'http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre130') tab2
      |JOIN    (SELECT object AS v1 , subject AS v0
      |FROM conductor
      |) tab0
      |ON(tab2.v0=tab0.v0)
      |JOIN    (SELECT subject AS v0 , object AS v2
      |FROM type
      |) tab1
      |ON(tab0.v0=tab1.v0)
    """.stripMargin


  val q20 =
    """
      |SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab1.v2 AS v2
      |FROM    (SELECT object AS v0
      |FROM likes
      |WHERE subject = "http://db.uwaterloo.ca/~galuc/wsdbm/User54768"
      |) tab2
      |JOIN    (SELECT subject AS v0 , object AS v2
      |FROM text
      |) tab1
      |ON(tab2.v0=tab1.v0)
      |JOIN    (SELECT object AS v1 , subject AS v0
      |FROM type
      |) tab0
      |ON(tab1.v0=tab0.v0)
    """.stripMargin

}
