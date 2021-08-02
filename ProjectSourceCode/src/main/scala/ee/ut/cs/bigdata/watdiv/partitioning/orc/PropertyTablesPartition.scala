package ee.ut.cs.bigdata.watdiv.partitioning.orc

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
      .appName("RDFBench ORC PT")
      .getOrCreate()
    import spark.implicits._

    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase //value = {"Horizontal", "Subject", or "Predicate"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/PT/"

    println("Start Partitioning PT tables WatDiv ORC...")

    //read tables from HDFS

    val Retailer_DF = spark.read.format("orc").load(path + "VHDFS/ORC/Retailer.orc")
    val Website_DF = spark.read.format("orc").load(path + "VHDFS/ORC/Website.orc")
    val Product_DF = spark.read.format("orc").load(path + "VHDFS/ORC/Product.orc")
    val User_DF = spark.read.format("orc").load(path + "VHDFS/ORC/User.orc")
    val Offer_DF = spark.read.format("orc").load(path + "VHDFS/ORC/Offer.orc")
    val City_DF = spark.read.format("orc").load(path + "VHDFS/ORC/City.orc")
    val Review_DF = spark.read.format("orc").load(path + "VHDFS/ORC/Review.orc")
    val Role_DF = spark.read.format("orc").load(path + "VHDFS/ORC/Role.orc")
    val Genre_DF = spark.read.format("orc").load(path + "VHDFS/ORC/Genre.orc")
    val Trailer_DF = spark.read.format("orc").load(path + "VHDFS/ORC/Trailer.orc")
    val Purchase_DF = spark.read.format("orc").load(path + "VHDFS/ORC/Purchase.orc")
    val Language_DF = spark.read.format("orc").load(path + "VHDFS/ORC/Language.orc")
    val Likes_DF = spark.read.format("orc").load(path + "VHDFS/ORC/Likes.orc")
    val Subscribes_DF = spark.read.format("orc").load(path + "VHDFS/ORC/Subscribes.orc")
    val EligibilityRegion_DF = spark.read.format("orc").load(path + "VHDFS/ORC/EligibilityRegion.orc")
    val HasGenre_DF = spark.read.format("orc").load(path + "VHDFS/ORC/HasGenre.orc")
    val MakesPurchase_DF = spark.read.format("orc").load(path + "VHDFS/ORC/MakesPurchase.orc")
    val Tag_DF = spark.read.format("orc").load(path + "VHDFS/ORC/Tag.orc")
    val Includes_DF = spark.read.format("orc").load(path + "VHDFS/ORC/Includes.orc")
    val Offers_DF = spark.read.format("orc").load(path + "VHDFS/ORC/Offers.orc")
    val HasReview_DF = spark.read.format("orc").load(path + "VHDFS/ORC/HasReview.orc")
    val FriendOf_DF = spark.read.format("orc").load(path + "VHDFS/ORC/FriendOf.orc")
    val Actor_DF = spark.read.format("orc").load(path + "VHDFS/ORC/Actor.orc")
    val PurchaseFor_DF = spark.read.format("orc").load(path + "VHDFS/ORC/PurchaseFor.orc")


    println("PT Tables Read!!")

    //partition and save on HDFS
    if (partitionType.toLowerCase == "subject") {
      Retailer_DF.repartition(84, $"retailer").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/Retailer.orc")
      Product_DF.repartition(84, $"product").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/Product.orc")
      User_DF.repartition(84, $"user").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/User.orc")
      Offer_DF.repartition(84, $"offer").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/Offer.orc")
      City_DF.repartition(84, $"city").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/City.orc")
      Review_DF.repartition(84, $"review").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/Review.orc")
      Website_DF.repartition(84, $"website").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/Website.orc")
      Role_DF.repartition(84, $"user").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/Role.orc")
      Genre_DF.repartition(84, $"subgenre").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/Genre.orc")
      Trailer_DF.repartition(84, $"product").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/Trailer.orc")
      Purchase_DF.repartition(84, $"purchase").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/Purchase.orc")
      Language_DF.repartition(84, $"product").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/Language.orc")
      Likes_DF.repartition(84, $"user").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/Likes.orc")
      Subscribes_DF.repartition(84, $"user").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/Subscribes.orc")
      EligibilityRegion_DF.repartition(84, $"offer").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/EligibilityRegion.orc")
      HasGenre_DF.repartition(84, $"product").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/HasGenre.orc")
      MakesPurchase_DF.repartition(84, $"user").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/MakesPurchase.orc")
      Tag_DF.repartition(84, $"product").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/Tag.orc")
      Includes_DF.repartition(84, $"offer").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/Includes.orc")
      Offers_DF.repartition(84, $"retailer").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/Offers.orc")
      HasReview_DF.repartition(84, $"product").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/HasReview.orc")
      FriendOf_DF.repartition(84, $"user1").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/FriendOf.orc")
      Actor_DF.repartition(84, $"product").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/Actor.orc")
      PurchaseFor_DF.repartition(84, $"purchase").write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Subject/ORC/PurchaseFor.orc")

      println("AVRO PT partitioned and saved! Subject based Partitioning!")

    }

    else if (partitionType == "horizontal") {

      Retailer_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/Retailer.orc")
      Product_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/Product.orc")
      User_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/User.orc")
      Offer_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/Offer.orc")
      City_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/City.orc")
      Review_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/Review.orc")
      Website_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/Website.orc")
      Role_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/Role.orc")
      Genre_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/Genre.orc")
      Trailer_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/Trailer.orc")
      Purchase_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/Purchase.orc")
      Language_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/Language.orc")
      Likes_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/Likes.orc")
      Subscribes_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/Subscribes.orc")
      EligibilityRegion_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/EligibilityRegion.orc")
      HasGenre_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/HasGenre.orc")
      MakesPurchase_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/MakesPurchase.orc")
      Tag_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/Tag.orc")
      Includes_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/Includes.orc")
      Offers_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/Offers.orc")
      HasReview_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/HasReview.orc")
      FriendOf_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/FriendOf.orc")
      Actor_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/Actor.orc")
      PurchaseFor_DF.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Horizontal/ORC/PurchaseFor.orc")


      println("AVRO PT partitioned and saved! Horizontal partitioning!")
    }

    else if (partitionType.toLowerCase == "predicate") {

      val purchaseDate = Purchase_DF.select("purchase", "purchaseDate").toDF()
      val purchasePrice = Purchase_DF.select("purchase", "price").toDF()
      val purchasePurchaseFor = Purchase_DF.select("purchase", "purchaseFor").toDF()

      purchaseDate.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/purchaseDate.orc")
      purchasePrice.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/purchasePrice.orc")
      purchasePurchaseFor.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/purchasePurchaseFor.orc")


      val reviewReviewer = Review_DF.select("review", "reviewer").toDF()
      val reviewRating = Review_DF.select("review", "rating").toDF()
      val reviewText = Review_DF.select("review", "text").toDF()
      val reviewTitle = Review_DF.select("review", "title").toDF()
      val reviewTotalVotes = Review_DF.select("review", "totalVotes").toDF()

      reviewReviewer.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/reviewReviewer.orc")
      reviewRating.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/reviewRating.orc")
      reviewText.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/reviewText.orc")
      reviewTitle.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/reviewTitle.orc")
      reviewTotalVotes.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/reviewTotalVotes.orc")


      val SubgenreGenre = Genre_DF.select("subgenre", "genre").toDF()
      val SubgenreTopic = Genre_DF.select("subgenre", "topic").toDF()

      SubgenreGenre.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/SubgenreGenre.orc")
      SubgenreTopic.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/SubgenreTopic.orc")

      val offerValidThrough = Offer_DF.select("offer", "validThrough").toDF()
      val offerELigibleQuantity = Offer_DF.select("offer", "eligibleQuantity").toDF()
      val offerValidFrom = Offer_DF.select("offer", "validFrom").toDF()
      val offerPrice = Offer_DF.select("offer", "price").toDF()
      val offerSerialNumber = Offer_DF.select("offer", "serialNumber").toDF()
      val offerPriceValidUntil = Offer_DF.select("offer", "priceValidUntil").toDF()

      offerValidThrough.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/offerValidThrough.orc")
      offerELigibleQuantity.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/offerELigibleQuantity.orc")
      offerValidFrom.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/offerValidFrom.orc")
      offerPrice.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/offerPrice.orc")
      offerSerialNumber.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/offerSerialNumber.orc")
      offerPriceValidUntil.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/offerPriceValidUntil.orc")


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

      productProductCategory.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productProductCategory.orc")
      productContentRating.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productContentRating.orc")
      productTitle.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productTitle.orc")
      productText.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productText.orc")
      productDescription.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productDescription.orc")
      productWordCount.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productWordCount.orc")
      productPrintPage.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productPrintPage.orc")
      productPublisher.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productPublisher.orc")
      productPrintSection.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productPrintSection.orc")
      productCaption.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productCaption.orc")
      productComposer.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productComposer.orc")
      productOpus.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productOpus.orc")
      productExpires.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productExpires.orc")
      productMovement.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productMovement.orc")
      productConductor.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productConductor.orc")
      productPerformer.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productPerformer.orc")
      productHomepage.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productHomepage.orc")
      productKeywords.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productKeywords.orc")
      productProducer.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productProducer.orc")
      productDuration.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productDuration.orc")
      productAward.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productAward.orc")
      productPerformed_in.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productPerformed_in.orc")
      productContentSize.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productContentSize.orc")
      productBookEdition.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productBookEdition.orc")
      productIsbn.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productIsbn.orc")
      productRelease.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productRelease.orc")
      productArtist.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productArtist.orc")
      productDatePublished.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productDatePublished.orc")
      productRecord_number.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productRecord_number.orc")
      productPrintEdition.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productPrintEdition.orc")
      productPrintColumn.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productPrintColumn.orc")
      productNumberOfPages.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/productNumberOfPages.orc")


      val websiteLanguage = Website_DF.select("website", "language").toDF()
      val websiteHits = Website_DF.select("website", "hits").toDF()
      val websiteUrl = Website_DF.select("website", "url").toDF()

      websiteLanguage.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/websiteLanguage.orc")
      websiteHits.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/websiteHits.orc")
      websiteUrl.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/websiteUrl.orc")

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

      userUserId.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/userUserId.orc")
      userGivenName.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/userGivenName.orc")
      userFamilyName.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/userFamilyName.orc")
      userEmail.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/userEmail.orc")
      userLocation.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/userLocation.orc")
      userGender.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/userGender.orc")
      userBirthDate.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/userBirthDate.orc")
      userAge.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/userAge.orc")
      userNationality.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/userNationality.orc")
      userTelephone.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/userTelephone.orc")
      userHomepage.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/userHomepage.orc")
      userJobTitle.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/userJobTitle.orc")


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

      retailerName.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/retailerName.orc")
      retailerLegalName.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/retailerLegalName.orc")
      retailerOpeningHours.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/retailerOpeningHours.orc")
      retailerDescription.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/retailerDescription.orc")
      retailerContactPoint.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/retailerContactPoint.orc")
      retailerTelephone.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/retailerTelephone.orc")
      retailerEmail.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/retailerEmail.orc")
      retailerPaymentAccepted.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/retailerPaymentAccepted.orc")
      retailerFaxNumber.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/retailerFaxNumber.orc")
      retailerAggregateRating.repartition(84).write.option("header", "true").format("orc").mode(SaveMode.Overwrite).save(path + "Predicate/ORC/retailerAggregateRating.orc")




      println("ORC PT partitioned and saved! Predicate based partitioning!")

    }


  }
}
