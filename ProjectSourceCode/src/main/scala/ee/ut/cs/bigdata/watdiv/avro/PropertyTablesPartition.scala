package ee.ut.cs.bigdata.watdiv.avro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
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
      .appName("RDFBench Avro PT")
      .getOrCreate()
    import spark.implicits._

    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase //value = {"Horizontal", "Subject", or "Predicate"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/PT/"

    println("Start Partitioning PT tables WatDiv Avro...")

    //read tables from HDFS

    val Retailer_DF = spark.read.format("avro").load(path + "VHDFS/Avro/Retailer.avro")
    val Website_DF = spark.read.format("avro").load(path + "VHDFS/Avro/Website.avro")
    val Product_DF = spark.read.format("avro").load(path + "VHDFS/Avro/Product.avro")
    val User_DF = spark.read.format("avro").load(path + "VHDFS/Avro/User.avro")
    val Offer_DF = spark.read.format("avro").load(path + "VHDFS/Avro/Offer.avro")
    val City_DF = spark.read.format("avro").load(path + "VHDFS/Avro/City.avro")
    val Review_DF = spark.read.format("avro").load(path + "VHDFS/Avro/Review.avro")
    val Role_DF = spark.read.format("avro").load(path + "VHDFS/Avro/Role.avro")
    val Genre_DF = spark.read.format("avro").load(path + "VHDFS/Avro/Genre.avro")
    val Trailer_DF = spark.read.format("avro").load(path + "VHDFS/Avro/Trailer.avro")
    val Purchase_DF = spark.read.format("avro").load(path + "VHDFS/Avro/Purchase.avro")
    val Language_DF = spark.read.format("avro").load(path + "VHDFS/Avro/Language.avro")
    val Likes_DF = spark.read.format("avro").load(path + "VHDFS/Avro/Likes.avro")
    val Subscribes_DF = spark.read.format("avro").load(path + "VHDFS/Avro/Subscribes.avro")
    val EligibilityRegion_DF = spark.read.format("avro").load(path + "VHDFS/Avro/EligibilityRegion.avro")
    val HasGenre_DF = spark.read.format("avro").load(path + "VHDFS/Avro/HasGenre.avro")
    val MakesPurchase_DF = spark.read.format("avro").load(path + "VHDFS/Avro/MakesPurchase.avro")
    val Tag_DF = spark.read.format("avro").load(path + "VHDFS/Avro/Tag.avro")
    val Includes_DF = spark.read.format("avro").load(path + "VHDFS/Avro/Includes.avro")
    val Offers_DF = spark.read.format("avro").load(path + "VHDFS/Avro/Offers.avro")
    val HasReview_DF = spark.read.format("avro").load(path + "VHDFS/Avro/HasReview.avro")
    val FriendOf_DF = spark.read.format("avro").load(path + "VHDFS/Avro/FriendOf.avro")
    val Actor_DF = spark.read.format("avro").load(path + "VHDFS/Avro/Actor.avro")
    val PurchaseFor_DF = spark.read.format("avro").load(path + "VHDFS/Avro/PurchaseFor.avro")


    println("PT Tables Read!!")

    //partition and save on HDFS
    if (partitionType.toLowerCase == "subject") {
      Retailer_DF.repartition(84, $"retailer").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/Retailer.avro")
      /*
      Product_DF.repartition(84, $"product").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/Product.avro")
      User_DF.repartition(84, $"user").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/User.avro")
      Offer_DF.repartition(84, $"offer").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/Offer.avro")
      City_DF.repartition(84, $"city").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/City.avro")
      Review_DF.repartition(84, $"review").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/Review.avro")
      Website_DF.repartition(84, $"website").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/Website.avro")
      Role_DF.repartition(84, $"user").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/Role.avro")
      Genre_DF.repartition(84, $"subgenre").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/Genre.avro")
      Trailer_DF.repartition(84, $"product").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/Trailer.avro")
      Purchase_DF.repartition(84, $"purchase").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/Purchase.avro")
      Language_DF.repartition(84, $"product").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/Language.avro")
      Likes_DF.repartition(84, $"user").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/Likes.avro")
      Subscribes_DF.repartition(84, $"user").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/Subscribes.avro")
      EligibilityRegion_DF.repartition(84, $"offer").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/EligibilityRegion.avro")
      HasGenre_DF.repartition(84, $"product").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/HasGenre.avro")
      MakesPurchase_DF.repartition(84, $"user").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/MakesPurchase.avro")
      Tag_DF.repartition(84, $"product").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/Tag.avro")
      Includes_DF.repartition(84, $"offer").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/Includes.avro")
      Offers_DF.repartition(84, $"retailer").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/Offers.avro")
      HasReview_DF.repartition(84, $"product").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/HasReview.avro")
      FriendOf_DF.repartition(84, $"user1").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/FriendOf.avro")
      Actor_DF.repartition(84, $"product").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/Actor.avro")
      PurchaseFor_DF.repartition(84, $"purchase").write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Subject/Avro/PurchaseFor.avro")

       */

      println("AVRO PT partitioned and saved! Subject based Partitioning!")

    }

    else if (partitionType == "horizontal") {

      Retailer_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/Retailer.avro")
      Product_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/Product.avro")
      User_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/User.avro")
      Offer_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/Offer.avro")
      City_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/City.avro")
      Review_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/Review.avro")
      Website_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/Website.avro")
      Role_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/Role.avro")
      Genre_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/Genre.avro")
      Trailer_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/Trailer.avro")
      Purchase_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/Purchase.avro")
      Language_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/Language.avro")
      Likes_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/Likes.avro")
      Subscribes_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/Subscribes.avro")
      EligibilityRegion_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/EligibilityRegion.avro")
      HasGenre_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/HasGenre.avro")
      MakesPurchase_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/MakesPurchase.avro")
      Tag_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/Tag.avro")
      Includes_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/Includes.avro")
      Offers_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/Offers.avro")
      HasReview_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/HasReview.avro")
      FriendOf_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/FriendOf.avro")
      Actor_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/Actor.avro")
      PurchaseFor_DF.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Horizontal/Avro/PurchaseFor.avro")


      println("AVRO PT partitioned and saved! Horizontal partitioning!")
    }

    else if (partitionType.toLowerCase == "predicate") {


      val purchaseDate = Purchase_DF.select("purchase", "purchaseDate").toDF()
      val purchasePrice = Purchase_DF.select("purchase", "price").toDF()
      val purchasePurchaseFor = Purchase_DF.select("purchase", "purchaseFor").toDF()

      purchaseDate.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/purchaseDate.avro")
      purchasePrice.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/purchasePrice.avro")
      purchasePurchaseFor.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/purchasePurchaseFor.avro")

      /*
      val reviewReviewer = Review_DF.select("review", "reviewer").toDF()
      val reviewRating = Review_DF.select("review", "rating").toDF()
      val reviewText = Review_DF.select("review", "text").toDF()
      val reviewTitle = Review_DF.select("review", "title").toDF()
      val reviewTotalVotes = Review_DF.select("review", "totalVotes").toDF()

      reviewReviewer.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/reviewReviewer.avro")
      reviewRating.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/reviewRating.avro")
      reviewText.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/reviewText.avro")
      reviewTitle.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/reviewTitle.avro")
      reviewTotalVotes.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/reviewTotalVotes.avro")

      val websiteLanguage = Website_DF.select("website", "language").toDF()
      val websiteHits = Website_DF.select("website", "hits").toDF()
      val websiteUrl = Website_DF.select("website", "url").toDF()

      websiteLanguage.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/websiteLanguage.avro")
      websiteHits.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/websiteHits.avro")
      websiteUrl.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/websiteUrl.avro")

      val SubgenreGenre = Genre_DF.select("subgenre", "genre").toDF()
      val SubgenreTopic = Genre_DF.select("subgenre", "topic").toDF()

      SubgenreGenre.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/SubgenreGenre.avro")
      SubgenreTopic.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/SubgenreTopic.avro")

      val offerValidThrough = Offer_DF.select("offer", "validThrough").toDF()
      val offerELigibleQuantity = Offer_DF.select("offer", "eligibleQuantity").toDF()
      val offerValidFrom = Offer_DF.select("offer", "validFrom").toDF()
      val offerPrice = Offer_DF.select("offer", "price").toDF()
      val offerSerialNumber = Offer_DF.select("offer", "serialNumber").toDF()
      val offerPriceValidUntil = Offer_DF.select("offer", "priceValidUntil").toDF()

      offerValidThrough.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/offerValidThrough.avro")
      offerELigibleQuantity.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/offerELigibleQuantity.avro")
      offerValidFrom.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/offerValidFrom.avro")
      offerPrice.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/offerPrice.avro")
      offerSerialNumber.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/offerSerialNumber.avro")
      offerPriceValidUntil.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/offerPriceValidUntil.avro")


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

      retailerName.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/retailerName.avro")
      retailerLegalName.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/retailerLegalName.avro")
      retailerOpeningHours.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/retailerOpeningHours.avro")
      retailerDescription.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/retailerDescription.avro")
      retailerContactPoint.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/retailerContactPoint.avro")
      retailerTelephone.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/retailerTelephone.avro")
      retailerEmail.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/retailerEmail.avro")
      retailerPaymentAccepted.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/retailerPaymentAccepted.avro")
      retailerFaxNumber.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/retailerFaxNumber.avro")
      retailerAggregateRating.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/retailerAggregateRating.avro")


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

      productProductCategory.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productProductCategory.avro")
      productContentRating.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productContentRating.avro")
      productTitle.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productTitle.avro")
      productText.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productText.avro")
      productDescription.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productDescription.avro")
      productWordCount.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productWordCount.avro")
      productPrintPage.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productPrintPage.avro")
      productPublisher.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productPublisher.avro")
      productPrintSection.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productPrintSection.avro")
      productCaption.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productCaption.avro")
      productComposer.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productComposer.avro")
      productOpus.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productOpus.avro")
      productExpires.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productExpires.avro")
      productMovement.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productMovement.avro")
      productConductor.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productConductor.avro")
      productPerformer.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productPerformer.avro")
      productHomepage.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productHomepage.avro")
      productKeywords.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productKeywords.avro")
      productProducer.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productProducer.avro")
      productDuration.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productDuration.avro")
      productAward.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productAward.avro")
      productPerformed_in.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productPerformed_in.avro")
      productContentSize.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productContentSize.avro")
      productBookEdition.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productBookEdition.avro")
      productIsbn.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productIsbn.avro")
      productRelease.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productRelease.avro")
      productArtist.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productArtist.avro")
      productDatePublished.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productDatePublished.avro")
      productRecord_number.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productRecord_number.avro")
      productPrintEdition.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productPrintEdition.avro")
      productPrintColumn.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productPrintColumn.avro")
      productNumberOfPages.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/productNumberOfPages.avro")

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

      userUserId.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/userUserId.avro")
      userGivenName.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/userGivenName.avro")
      userFamilyName.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/userFamilyName.avro")
      userEmail.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/userEmail.avro")
      userLocation.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/userLocation.avro")
      userGender.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/userGender.avro")
      userBirthDate.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/userBirthDate.avro")
      userAge.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/userAge.avro")
      userNationality.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/userNationality.avro")
      userTelephone.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/userTelephone.avro")
      userHomepage.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/userHomepage.avro")
      userJobTitle.repartition(84).write.option("header", "true").format("avro").mode(SaveMode.Overwrite).save(path + "Predicate/Avro/userJobTitle.avro")

       */


      println("Avro PT partitioned and saved! Predicate based partitioning!")

    }


  }
}
