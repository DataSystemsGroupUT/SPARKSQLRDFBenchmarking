package ee.ut.cs.bigdata.watdiv.queries

class PTQueries {
  //Complex

  val q1 =
    """
      |SELECT Product1.product, Review.review, User.user, Product2.product
      |FROM Product AS Product1
      |JOIN HasReview ON Product1.product =HasReview.product
      |JOIN Review ON HasReview.review = Review.review
      |JOIN User ON Review.reviewer=User.user
      |
      |
      |JOIN Product AS Product2
      |JOIN Actor ON Product2.product=Actor.product AND Actor.actor=User.user
      |JOIN Language ON Product2.product = Language.product
      |WHERE Product1.contentRating !=''
      |AND Review.title != ''
      |AND Product1.text !=''
      |AND Product1.caption !=''
  """.stripMargin

  val q2 =
    """
      |SELECT Retailer.retailer, Includes.product, MakesPurchase.user, Review.review
      |FROM Retailer
      |JOIN Offers ON Retailer.retailer=Offers.retailer
      |JOIN EligibilityRegion ON Offers.offer=EligibilityRegion.offer
      |JOIN Includes ON Includes.offer= Offers.offer
      |JOIN HasReview On Includes.product= HasReview.product
      |JOIN Review ON HasReview.review= Review.review
      |JOIN MakesPurchase
      |JOIN Purchase ON MakesPurchase.purchase=Purchase.purchase
      |JOIN PurchaseFor ON PurchaseFor.product = Includes.product AND PurchaseFor.purchase=Purchase.purchase
      |JOIN User ON MakesPurchase.user=User.user
      |WHERE  EligibilityRegion.country="http://db.uwaterloo.ca/~galuc/wsdbm/Country3"
      |AND Retailer.legalName !=''
      |AND User.homepage !=''
      |AND User.jobTitle !=''
      |AND Review.totalVotes !=''
""".stripMargin

  val q3 =
    """
      |SELECT  User.user
      |FROM User
      |JOIN Likes On Likes.user=User.user
      |JOIN FriendOf ON User.user=FriendOf.user1
      |WHERE User.age !=''
      |AND User.gender !=''
      |AND User.givenName !=''
      |AND User.location !=''
  """.stripMargin

  // Snow-Flake (F)

  val q4 =
    """
      |SELECT Genre.subgenre, Genre.genre, Product.product, Trailer.trailer, Product.keywords
      |FROM Product
      |JOIN Trailer ON Product.product =Trailer.product
      |JOIN HasGenre ON HasGenre.product = Product.product
      |JOIN Genre ON HasGenre.subgenre=Genre.subgenre
      |WHERE Genre.topic="http://db.uwaterloo.ca/~galuc/wsdbm/Topic76"
      |AND Product.keywords !=""
      |AND Product.productCategory="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2"
    """.stripMargin

  val q5 =
    """
      |SELECT Product.product, Website.website, Product.title, Product.caption, Product.description, Website.url, Website.hits
      |FROM Product
      |JOIN HasGenre ON  HasGenre.product = Product.product
      |JOIN Website ON Product.homepage= Website.website
      |WHERE HasGenre.subgenre="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre100"
      |AND Product.caption != ''
      |AND Product.description !=''
    """.stripMargin

  val q6 =

    """
      |SELECT Product.product, Product.contentRating, Product.contentSize, MakesPurchase.user, Purchase.purchase, Purchase.purchaseDate
      |FROM Product
      |JOIN HasGenre ON  HasGenre.product = Product.product
      |JOIN PurchaseFor ON Product.product = PurchaseFor.product
      |JOIN MakesPurchase ON PurchaseFor.purchase = MakesPurchase.purchase
      |JOIN Purchase  ON Purchase.purchase = MakesPurchase.purchase
      |WHERE HasGenre.subgenre="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre70"
      |AND Product.contentSize != ''
      |AND Product.contentRating != ''
      |AND Purchase.purchaseDate !=''
  """.stripMargin

  val q7 =

    """
      |SELECT Product.product, Product.homepage, Includes.offer, Product.description, Website.url, Website.hits, Likes.user, Product.contentSize
      |FROM Product
      |JOIN Tag ON Product.product=Tag.product
      |JOIN Website ON  Website.website= Product.homepage
      |JOIN Includes ON Includes.product=Product.product
      |JOIN Likes ON Likes.product=Product.product
      |WHERE Tag.topic="http://db.uwaterloo.ca/~galuc/wsdbm/Topic231"
      |AND Website.language="http://db.uwaterloo.ca/~galuc/wsdbm/Language0"
      |AND Product.contentSize != ''
      |AND Product.description !=''
    """.stripMargin

  val q8 =
    """
      |SELECT Offer.offer, Product.product, Offer.price, Offer.validThrough, Product.title, Product.productCategory
      |FROM Offer
      |JOIN Offers ON Offer.offer=Offers.offer
      |JOIN Includes ON Offer.offer=Includes.offer
      |JOIN Product ON Product.product=Includes.product
      |WHERE Offers.retailer ="http://db.uwaterloo.ca/~galuc/wsdbm/Retailer113"
    """.stripMargin

  // Linear (L)


  val q9 =
    """
      |SELECT  Subscribes.user, Likes.product, Product.caption
      |FROM Subscribes
      |JOIN  Likes ON Likes.user = Subscribes.user AND Subscribes.website="http://db.uwaterloo.ca/~galuc/wsdbm/Website342"
      |JOIN  Product ON Likes.product=Product.product
      |WHERE Product.caption !=''
    """.stripMargin


  val q10 =
    """
      |SELECT User.user, City.parentCountry
      |FROM City
      |JOIN User ON user.nationality=City.parentcountry
      |JOIN Likes ON User.user=Likes.user
      |WHERE City.city="http://db.uwaterloo.ca/~galuc/wsdbm/City152"
      |AND Likes.product="http://db.uwaterloo.ca/~galuc/wsdbm/Product0"
    """.stripMargin


  val q11 =
    """
      |SELECT Likes.product, Subscribes.user
      |FROM Subscribes
      |JOIN  Likes ON Likes.user = Subscribes.user
      |AND Subscribes.website="http://db.uwaterloo.ca/~galuc/wsdbm/Website34"
    """.stripMargin


  val q12 =
    """
      |SELECT Product.product, Product.caption
      |FROM Product
      |JOIN Tag ON Product.product=Tag.product
      |WHERE Tag.topic="http://db.uwaterloo.ca/~galuc/wsdbm/Topic134"
      |AND Product.caption !=""
    """.stripMargin


  val q13 =
    """
      |SELECT User.user, User.jobTitle, City.parentCountry
      |FROM  User
      |JOIN  City ON User.nationality=City.parentCountry
      |WHERE City.city="http://db.uwaterloo.ca/~galuc/wsdbm/City112"
      |AND User.jobTitle !=''
    """.stripMargin


  //Star (S)


  val q14 =
    """
      |SELECT Offer.offer, Includes.product, Offer.price, Offer.serialnumber, Offer.validFrom,
      |Offer.validThrough, Offer.eligibleQuantity, EligibilityRegion.country, Offer.priceValidUntil
      |FROM Offers
      |JOIN Offer ON Offers.offer=Offer.offer
      |JOIN Includes ON Includes.offer=Offer.offer
      |JOIN EligibilityRegion ON Offer.offer=EligibilityRegion.offer
      |WHERE Offers.retailer="http://db.uwaterloo.ca/~galuc/wsdbm/Retailer4"
      |AND Offer.validFrom !=''
      |AND Offer.priceValidUntil !=''
    """.stripMargin


  val q15 =
    """
      |SELECT User.user, user.location, User.gender
      |FROM User
      |JOIN Role ON User.user=Role.user
      |WHERE User.nationality="http://db.uwaterloo.ca/~galuc/wsdbm/Country12"
      |AND Role.role="http://db.uwaterloo.ca/~galuc/wsdbm/Role2"
      |AND User.gender!=''
    """.stripMargin


  val q16 =
    """
      |SELECT Product.product, Product.caption, HasGenre.subgenre, Product.publisher
      |FROM Product
      |JOIN HasGenre ON Product.product = HasGenre.product
      |WHERE  Product.productCategory="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory9"
      |AND Product.caption !=''
      |AND Product.publisher !=''
    """.stripMargin


  val q17 =
    """
      |SELECT User.user, Product.product, User.familyName
      |FROM User
      |JOIN Product ON Product.artist=User.user
      |WHERE User.age="http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup5"
      |AND User.nationality="http://db.uwaterloo.ca/~galuc/wsdbm/Country1"
    """.stripMargin


  val q18 =
    """
      |SELECT Product.product, Product.description, Product.keywords
      |FROM Product
      |JOIN Language On Language.product= Product.product
      |WHERE Product.productCategory="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory3"
      |AND Language.language="http://db.uwaterloo.ca/~galuc/wsdbm/Language0"
      |AND Product.description !=''
      |AND Product.keywords !=''
    """.stripMargin


  val q19 =
    """
      |SELECT  Product.product, Product.conductor, Product.productCategory
      |FROM Product
      |JOIN HasGenre On HasGenre.product = Product.product
      |WHERE HasGenre.subgenre="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre72"
      |AND Product.conductor !=''
    """.stripMargin


  val q20 =
    """
      |SELECT Product.product, Product.productCategory, Product.text
      |FROM Product
      |JOIN Likes ON Likes.product = Product.product
      |WHERE Likes.user="http://db.uwaterloo.ca/~galuc/wsdbm/User5641"
      |AND Product.text !=''
    """.stripMargin
}