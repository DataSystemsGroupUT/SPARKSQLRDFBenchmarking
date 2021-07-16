package ee.ut.cs.bigdata.watdiv.queries

class PTQueries {
  //Complex

  val c1 =
    """
      |SELECT Product1.product, Review.review, User.user, Product2.product
      |FROM Product AS Product1
      |JOIN HasReview ON Product1.product =HasReview.product
      |JOIN Review ON HasReview.review = Review.review
      |JOIN User ON Review.reviewer=User.user
      |JOIN Actor ON Actor.actor=User.user
      |JOIN Product AS Product2 ON Product2.product=Actor.product
      |JOIN Language ON Product2.product = Language.product
      |WHERE Product1.contentRating IS NOT NULL
      |AND Review.title IS NOT NULL
      |AND Product1.text IS NOT NULL
      |AND Product1.caption IS NOT NULL
  """.stripMargin


  val c2 =
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
      |AND Retailer.legalName IS NOT NULL
      |AND User.homepage IS NOT NULL
      |AND User.jobTitle IS NOT NULL
      |AND Review.totalVotes IS NOT NULL
""".stripMargin

  val c3 =
    """
      |SELECT  User.user
      |FROM User
      |JOIN Likes On Likes.user=User.user
      |JOIN FriendOf ON User.user=FriendOf.user1
      |WHERE User.age IS NOT NULL
      |AND User.gender IS NOT NULL
      |AND User.givenName IS NOT NULL
      |AND User.location IS NOT NULL
  """.stripMargin



  // Snow-Flake (F)

  val f1 =
    """
      |SELECT Genre.subgenre, Genre.genre, Product.product, Trailer.trailer, Product.keywords
      |FROM Product
      |JOIN Trailer ON Product.product =Trailer.product
      |JOIN HasGenre ON HasGenre.product = Product.product
      |JOIN Genre ON HasGenre.subgenre=Genre.subgenre
      |WHERE Genre.topic="http://db.uwaterloo.ca/~galuc/wsdbm/Topic8"
      |AND Product.keywords IS NOT NULL
      |AND Product.productCategory="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2"
    """.stripMargin

  val f2 =
    """
      |SELECT Product.product, Website.website, Product.title, Product.caption, Product.description, Website.url, Website.hits
      |FROM Product
      |JOIN HasGenre ON  HasGenre.product = Product.product
      |JOIN Website ON Product.homepage= Website.website
      |WHERE HasGenre.subgenre="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre117"
      |AND Product.caption IS NOT NULL
      |AND Product.description IS NOT NULL
    """.stripMargin

  val f3 =

    """
      |SELECT Product.product, Product.contentRating, Product.contentSize, MakesPurchase.user, Purchase.purchase, Purchase.purchaseDate
      |FROM Product
      |JOIN HasGenre ON  HasGenre.product = Product.product
      |JOIN PurchaseFor ON Product.product = PurchaseFor.product
      |JOIN MakesPurchase ON PurchaseFor.purchase = MakesPurchase.purchase
      |JOIN Purchase  ON Purchase.purchase = MakesPurchase.purchase
      |WHERE HasGenre.subgenre="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre111"
      |AND Product.contentSize IS NOT NULL
      |AND Product.contentRating IS NOT NULL
      |AND Purchase.purchaseDate IS NOT NULL
  """.stripMargin

  val f4 =

    """
      |SELECT Product.product, Product.homepage, Includes.offer, Product.description, Website.url, Website.hits, Likes.user, Product.contentSize
      |FROM Product
      |JOIN Tag ON Product.product=Tag.product
      |JOIN Website ON  Website.website= Product.homepage
      |JOIN Includes ON Includes.product=Product.product
      |JOIN Likes ON Likes.product=Product.product
      |WHERE Tag.topic="http://db.uwaterloo.ca/~galuc/wsdbm/Topic122"
      |AND Website.language="http://db.uwaterloo.ca/~galuc/wsdbm/Language0"
      |AND Product.contentSize IS NOT NULL
      |AND Product.description IS NOT NULL
    """.stripMargin

  val f5 =
    """
      |SELECT Offer.offer, Product.product, Offer.price, Offer.validThrough, Product.title, Product.productCategory
      |FROM Offer
      |JOIN Offers ON Offer.offer=Offers.offer
      |JOIN Includes ON Offer.offer=Includes.offer
      |JOIN Product ON Product.product=Includes.product
      |WHERE Offers.retailer ="http://db.uwaterloo.ca/~galuc/wsdbm/Retailer9885"
    """.stripMargin

  // Linear (L)


  val l1 =
    """
      |SELECT  Subscribes.user, Likes.product, Product.caption
      |FROM Subscribes
      |JOIN  Likes ON Likes.user = Subscribes.user AND Subscribes.website="http://db.uwaterloo.ca/~galuc/wsdbm/Website7355"
      |JOIN  Product ON Likes.product=Product.product
      |WHERE Product.caption IS NOT NULL
    """.stripMargin


  val l2 =
    """
      |SELECT User.user, City.parentCountry
      |FROM City
      |JOIN User ON user.nationality=City.parentCountry
      |JOIN Likes ON User.user=Likes.user
      |WHERE City.city="http://db.uwaterloo.ca/~galuc/wsdbm/City70"
      |AND Likes.product="http://db.uwaterloo.ca/~galuc/wsdbm/Product0"
    """.stripMargin


  val l3 =
    """
      |SELECT Likes.product, Subscribes.user
      |FROM Subscribes
      |JOIN  Likes ON Likes.user = Subscribes.user
      |AND Subscribes.website="http://db.uwaterloo.ca/~galuc/wsdbm/Website43164"
    """.stripMargin


  val l4 =
    """
      |SELECT Product.product, Product.caption
      |FROM Product
      |JOIN Tag ON Product.product=Tag.product
      |WHERE Tag.topic="http://db.uwaterloo.ca/~galuc/wsdbm/Topic142"
      |AND Product.caption IS NOT NULL
    """.stripMargin


  val l5 =
    """
      |SELECT User.user, User.jobTitle, City.parentCountry
      |FROM  User
      |JOIN  City ON User.nationality=City.parentCountry
      |WHERE City.city="http://db.uwaterloo.ca/~galuc/wsdbm/City40"
      |AND User.jobTitle IS NOT NULL
    """.stripMargin


  //Star (S)


  val s1 =
    """
      |SELECT Offer.offer, Includes.product, Offer.price, Offer.serialnumber, Offer.validFrom,
      |Offer.validThrough, Offer.eligibleQuantity, EligibilityRegion.country, Offer.priceValidUntil
      |FROM Offers
      |JOIN Offer ON Offers.offer=Offer.offer
      |JOIN Includes ON Includes.offer=Offer.offer
      |JOIN EligibilityRegion ON Offer.offer=EligibilityRegion.offer
      |WHERE Offers.retailer="http://db.uwaterloo.ca/~galuc/wsdbm/Retailer8535"
      |AND Offer.validFrom IS NOT NULL
      |AND Offer.priceValidUntil IS NOT NULL
    """.stripMargin


  val s2 =
    """
      |SELECT DISTINCT User.user, user.location, User.gender
      |FROM User
      |JOIN Role ON User.user=Role.user
      |WHERE User.nationality="http://db.uwaterloo.ca/~galuc/wsdbm/Country4"
      |AND Role.role="http://db.uwaterloo.ca/~galuc/wsdbm/Role2"
      |AND User.gender IS NOT NULL
      |AND User.location IS NOT NULL
    """.stripMargin


  val s3 =
    """
      |SELECT Product.product, Product.caption, HasGenre.subgenre, Product.publisher
      |FROM Product
      |JOIN HasGenre ON Product.product = HasGenre.product
      |WHERE  Product.productCategory="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory4"
      |AND Product.caption IS NOT NULL
      |AND Product.publisher IS NOT NULL
    """.stripMargin


  val s4 =
    """
      |SELECT User.user, Product.product, User.familyName
      |FROM User
      |JOIN Product ON Product.artist=User.user
      |WHERE User.age="http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup5"
      |AND User.nationality="http://db.uwaterloo.ca/~galuc/wsdbm/Country1"
      |AND user.familyName IS NOT NULL
    """.stripMargin


  val s5 =
    """
      |SELECT Product.product, Product.description, Product.keywords
      |FROM Product
      |JOIN Language On Language.product= Product.product
      |WHERE Product.productCategory="http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory3"
      |AND Language.language="http://db.uwaterloo.ca/~galuc/wsdbm/Language0"
      |AND Product.description IS NOT NULL
      |AND Product.keywords IS NOT NULL
    """.stripMargin


  val s6 =
    """
      |SELECT  Product.product, Product.conductor, Product.productCategory
      |FROM Product
      |JOIN HasGenre On HasGenre.product = Product.product
      |WHERE HasGenre.subgenre="http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre130"
      |AND Product.conductor IS NOT NULL
    """.stripMargin


  val s7 =
    """
      |SELECT Product.product, Product.productCategory, Product.text
      |FROM Product
      |JOIN Likes ON Likes.product = Product.product
      |WHERE Likes.user="http://db.uwaterloo.ca/~galuc/wsdbm/User54768"
      |AND Product.text IS NOT NULL
    """.stripMargin
}
