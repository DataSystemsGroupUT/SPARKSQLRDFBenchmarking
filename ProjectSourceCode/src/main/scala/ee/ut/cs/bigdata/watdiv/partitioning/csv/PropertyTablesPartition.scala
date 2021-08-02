package ee.ut.cs.bigdata.watdiv.partitioning.csv

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object PropertyTablesPartition {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("RDFBench CSV PT")
      .getOrCreate()
    import spark.implicits._

    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase //value = {"Horizontal", "Subject", or "Predicate"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/PT/"

    println("Start Partitioning PT tables WatDiv CSV...")

    //read tables from HDFS

    val Retailer_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/Retailer.csv")
    val Website_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/Website.csv")
    val Product_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/Product.csv")
    val User_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/User.csv")
    val Offer_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/Offer.csv")
    val City_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/City.csv")
    val Review_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/Review.csv")
    val Role_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/Role.csv")
    val Genre_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/Genre.csv")
    val Trailer_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/Trailer.csv")
    val Purchase_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/Purchase.csv")
    val Language_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/Language.csv")
    val Likes_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/Likes.csv")
    val Subscribes_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/Subscribes.csv")
    val EligibilityRegion_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/EligibilityRegion.csv")
    val HasGenre_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/HasGenre.csv")
    val MakesPurchase_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/MakesPurchase.csv")
    val Tag_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/Tag.csv")
    val Includes_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/Includes.csv")
    val Offers_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/Offers.csv")
    val HasReview_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/HasReview.csv")
    val FriendOf_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/FriendOf.csv")
    val Actor_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/Actor.csv")
    val PurchaseFor_DF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path + "VHDFS/CSV/PurchaseFor.csv")


    println("PT Tables Read!!")

    //partition and save on HDFS
    if (partitionType.toLowerCase == "subject") {
      Retailer_DF.repartition(84, $"retailer").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/Retailer.csv")
      Product_DF.repartition(84, $"product").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/Product.csv")
      User_DF.repartition(84, $"user").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/User.csv")
      Offer_DF.repartition(84, $"offer").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/Offer.csv")
      City_DF.repartition(84, $"city").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/City.csv")
      Review_DF.repartition(84, $"review").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/Review.csv")
      Website_DF.repartition(84, $"website").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/Website.csv")
      Role_DF.repartition(84, $"user").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/Role.csv")
      Genre_DF.repartition(84, $"subgenre").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/Genre.csv")
      Trailer_DF.repartition(84, $"product").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/Trailer.csv")
      Purchase_DF.repartition(84, $"purchase").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/Purchase.csv")
      Language_DF.repartition(84, $"product").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/Language.csv")
      Likes_DF.repartition(84, $"user").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/Likes.csv")
      Subscribes_DF.repartition(84, $"user").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/Subscribes.csv")
      EligibilityRegion_DF.repartition(84, $"offer").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/EligibilityRegion.csv")
      HasGenre_DF.repartition(84, $"product").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/HasGenre.csv")
      MakesPurchase_DF.repartition(84, $"user").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/MakesPurchase.csv")
      Tag_DF.repartition(84, $"product").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/Tag.csv")
      Includes_DF.repartition(84, $"offer").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/Includes.csv")
      Offers_DF.repartition(84, $"retailer").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/Offers.csv")
      HasReview_DF.repartition(84, $"product").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/HasReview.csv")
      FriendOf_DF.repartition(84, $"user1").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/FriendOf.csv")
      Actor_DF.repartition(84, $"product").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/Actor.csv")
      PurchaseFor_DF.repartition(84, $"purchase").write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Subject/CSV/PurchaseFor.csv")

      println("AVRO PT partitioned and saved! Subject based Partitioning!")

    }

    else if (partitionType == "horizontal") {

      Retailer_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/Retailer.csv")
      Product_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/Product.csv")
      User_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/User.csv")
      Offer_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/Offer.csv")
      City_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/City.csv")
      Review_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/Review.csv")
      Website_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/Website.csv")
      Role_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/Role.csv")
      Genre_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/Genre.csv")
      Trailer_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/Trailer.csv")
      Purchase_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/Purchase.csv")
      Language_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/Language.csv")
      Likes_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/Likes.csv")
      Subscribes_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/Subscribes.csv")
      EligibilityRegion_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/EligibilityRegion.csv")
      HasGenre_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/HasGenre.csv")
      MakesPurchase_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/MakesPurchase.csv")
      Tag_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/Tag.csv")
      Includes_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/Includes.csv")
      Offers_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/Offers.csv")
      HasReview_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/HasReview.csv")
      FriendOf_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/FriendOf.csv")
      Actor_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/Actor.csv")
      PurchaseFor_DF.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Horizontal/CSV/PurchaseFor.csv")


      println("AVRO PT partitioned and saved! Horizontal partitioning!")
    }

    else if (partitionType.toLowerCase == "predicate") {

      val purchaseDate = Purchase_DF.select("purchase", "purchaseDate").toDF()
      val purchasePrice = Purchase_DF.select("purchase", "price").toDF()
      val purchasePurchaseFor = Purchase_DF.select("purchase", "purchaseFor").toDF()

      purchaseDate.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/purchaseDate.csv")
      purchasePrice.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/purchasePrice.csv")
      purchasePurchaseFor.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/purchasePurchaseFor.csv")

      /*
      val reviewReviewer = Review_DF.select("review", "reviewer").toDF()
      val reviewRating = Review_DF.select("review", "rating").toDF()
      val reviewText = Review_DF.select("review", "text").toDF()
      val reviewTitle = Review_DF.select("review", "title").toDF()
      val reviewTotalVotes = Review_DF.select("review", "totalVotes").toDF()

      reviewReviewer.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/reviewReviewer.csv")
      reviewRating.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/reviewRating.csv")
      reviewText.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/reviewText.csv")
      reviewTitle.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/reviewTitle.csv")
      reviewTotalVotes.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/reviewTotalVotes.csv")


      val SubgenreGenre = Genre_DF.select("subgenre", "genre").toDF()
      val SubgenreTopic = Genre_DF.select("subgenre", "topic").toDF()

      SubgenreGenre.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/SubgenreGenre.csv")
      SubgenreTopic.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/SubgenreTopic.csv")

      val offerValidThrough = Offer_DF.select("offer", "validThrough").toDF()
      val offerELigibleQuantity = Offer_DF.select("offer", "eligibleQuantity").toDF()
      val offerValidFrom = Offer_DF.select("offer", "validFrom").toDF()
      val offerPrice = Offer_DF.select("offer", "price").toDF()
      val offerSerialNumber = Offer_DF.select("offer", "serialNumber").toDF()
      val offerPriceValidUntil = Offer_DF.select("offer", "priceValidUntil").toDF()

      offerValidThrough.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/offerValidThrough.csv")
      offerELigibleQuantity.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/offerELigibleQuantity.csv")
      offerValidFrom.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/offerValidFrom.csv")
      offerPrice.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/offerPrice.csv")
      offerSerialNumber.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/offerSerialNumber.csv")
      offerPriceValidUntil.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/offerPriceValidUntil.csv")


      val productProductCategory = Product_DF.select("product", "productcategory").toDF()
      val productContentRating = Product_DF.select("product", "contentRating").toDF()
      val productTitle = Product_DF.select("product", "title").toDF()
      val productText = Product_DF.select("product", "text").toDF()
      val productDescription = Product_DF.select("product", "description").toDF()
      val productWordCount = Product_DF.select("product", "wordCount").toDF()
      val productPrintPage = Product_DF.select("product", "printPage").toDF()
      val productPublisher = Product_DF.select("product", "publisher").toDF()
      val productPrintSection = Product_DF.select("product", "printSection").toDF()
      val productCaption = Product_DF.select("product", "caption").toDF()
      val productComposer = Product_DF.select("product", "composer").toDF()
      val productOpus = Product_DF.select("product", "opus").toDF()
      val productExpires = Product_DF.select("product", "expires").toDF()
      val productMovement = Product_DF.select("product", "movement").toDF()
      val productConductor = Product_DF.select("product", "conductor").toDF()
      val productPerformer = Product_DF.select("product", "performer").toDF()
      val productHomepage = Product_DF.select("product", "homepage").toDF()
      val productKeywords = Product_DF.select("product", "keywords").toDF()
      val productProducer = Product_DF.select("product", "producer").toDF()
      val productDuration = Product_DF.select("product", "duration").toDF()
      val productAward = Product_DF.select("product", "award").toDF()
      val productPerformed_in = Product_DF.select("product", "performed_in").toDF()
      val productContentSize = Product_DF.select("product", "contentSize").toDF()
      val productBookEdition = Product_DF.select("product", "bookEdition").toDF()
      val productIsbn = Product_DF.select("product", "isbn").toDF()
      val productRelease = Product_DF.select("product", "release").toDF()
      val productArtist = Product_DF.select("product", "artist").toDF()
      val productDatePublished = Product_DF.select("product", "datePublished").toDF()
      val productRecord_number = Product_DF.select("product", "record_number").toDF()
      val productPrintEdition = Product_DF.select("product", "printEdition").toDF()
      val productPrintColumn = Product_DF.select("product", "printColumn").toDF()
      val productNumberOfPages = Product_DF.select("product", "numberOfPages").toDF()

      productProductCategory.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productProductCategory.csv")
      productContentRating.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productContentRating.csv")
      productTitle.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productTitle.csv")
      productText.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productText.csv")
      productDescription.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productDescription.csv")
      productWordCount.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productWordCount.csv")
      productPrintPage.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productPrintPage.csv")
      productPublisher.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productPublisher.csv")
      productPrintSection.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productPrintSection.csv")
      productCaption.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productCaption.csv")
      productComposer.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productComposer.csv")
      productOpus.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productOpus.csv")
      productExpires.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productExpires.csv")
      productMovement.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productMovement.csv")
      productConductor.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productConductor.csv")
      productPerformer.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productPerformer.csv")
      productHomepage.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productHomepage.csv")
      productKeywords.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productKeywords.csv")
      productProducer.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productProducer.csv")
      productDuration.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productDuration.csv")
      productAward.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productAward.csv")
      productPerformed_in.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productPerformed_in.csv")
      productContentSize.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productContentSize.csv")
      productBookEdition.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productBookEdition.csv")
      productIsbn.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productIsbn.csv")
      productRelease.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productRelease.csv")
      productArtist.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productArtist.csv")
      productDatePublished.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productDatePublished.csv")
      productRecord_number.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productRecord_number.csv")
      productPrintEdition.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productPrintEdition.csv")
      productPrintColumn.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productPrintColumn.csv")
      productNumberOfPages.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/productNumberOfPages.csv")


      val websiteLanguage = Website_DF.select("website", "language").toDF()
      val websiteHits = Website_DF.select("website", "hits").toDF()
      val websiteUrl = Website_DF.select("website", "url").toDF()

      websiteLanguage.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/websiteLanguage.csv")
      websiteHits.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/websiteHits.csv")
      websiteUrl.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/websiteUrl.csv")

      val userUserId = User_DF.select("user", "userId").toDF()
      val userGivenName = User_DF.select("user", "givenName").toDF()
      val userFamilyName = User_DF.select("user", "familyName").toDF()
      val userEmail = User_DF.select("user", "email").toDF()
      val userLocation = User_DF.select("user", "Location").toDF()
      val userGender = User_DF.select("user", "gender").toDF()
      val userBirthDate = User_DF.select("user", "birthDate").toDF()
      val userAge = User_DF.select("user", "age").toDF()
      val userNationality = User_DF.select("user", "nationality").toDF()
      val userTelephone = User_DF.select("user", "telephone").toDF()
      val userHomepage = User_DF.select("user", "homepage").toDF()
      val userJobTitle = User_DF.select("user", "jobTitle").toDF()

      userUserId.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/userUserId.csv")
      userGivenName.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/userGivenName.csv")
      userFamilyName.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/userFamilyName.csv")
      userEmail.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/userEmail.csv")
      userLocation.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/userLocation.csv")
      userGender.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/userGender.csv")
      userBirthDate.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/userBirthDate.csv")
      userAge.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/userAge.csv")
      userNationality.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/userNationality.csv")
      userTelephone.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/userTelephone.csv")
      userHomepage.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/userHomepage.csv")
      userJobTitle.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/userJobTitle.csv")


      val retailerName = Retailer_DF.select("retailer", "name").toDF()
      val retailerLegalName = Retailer_DF.select("retailer", "legalName").toDF()
      val retailerOpeningHours = Retailer_DF.select("retailer", "openingHours").toDF()
      val retailerDescription = Retailer_DF.select("retailer", "description").toDF()
      val retailerContactPoint = Retailer_DF.select("retailer", "contactPoint").toDF()
      val retailerTelephone = Retailer_DF.select("retailer", "telephone").toDF()
      val retailerEmail = Retailer_DF.select("retailer", "email").toDF()
      val retailerPaymentAccepted = Retailer_DF.select("retailer", "paymentAccepted").toDF()
      val retailerFaxNumber = Retailer_DF.select("retailer", "faxNumber").toDF()
      val retailerAggregateRating = Retailer_DF.select("retailer", "aggregateRating").toDF()

      retailerName.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/retailerName.csv")
      retailerLegalName.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/retailerLegalName.csv")
      retailerOpeningHours.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/retailerOpeningHours.csv")
      retailerDescription.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/retailerDescription.csv")
      retailerContactPoint.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/retailerContactPoint.csv")
      retailerTelephone.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/retailerTelephone.csv")
      retailerEmail.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/retailerEmail.csv")
      retailerPaymentAccepted.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/retailerPaymentAccepted.csv")
      retailerFaxNumber.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/retailerFaxNumber.csv")
      retailerAggregateRating.repartition(84).write.option("header", "true").format("csv").mode(SaveMode.Overwrite).save(path + "Predicate/CSV/retailerAggregateRating.csv")

       */


      println("CSV PT partitioned and saved! Predicate based partitioning!")

    }


  }
}
