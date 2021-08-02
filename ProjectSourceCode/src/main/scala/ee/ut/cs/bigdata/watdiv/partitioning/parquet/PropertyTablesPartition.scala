package ee.ut.cs.bigdata.watdiv.partitioning.parquet

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
      .appName("RDFBench Parquet PT")
      .getOrCreate()
    import spark.implicits._

    val ds = args(0) //value = {"100M", "500M, or "1B"}
    val partitionType = args(1).toLowerCase //value = {"Horizontal", "Subject", or "Predicate"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/PT/"

    println("Start Partitioning PT tables WatDiv Parquet...")

    //read tables from HDFS

    val Retailer_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/Retailer.parquet")
    val Website_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/Website.parquet")
    val Product_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/Product.parquet")
    val User_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/User.parquet")
    val Offer_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/Offer.parquet")
    val City_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/City.parquet")
    val Review_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/Review.parquet")
    val Role_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/Role.parquet")
    val Genre_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/Genre.parquet")
    val Trailer_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/Trailer.parquet")
    val Purchase_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/Purchase.parquet")
    val Language_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/Language.parquet")
    val Likes_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/Likes.parquet")
    val Subscribes_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/Subscribes.parquet")
    val EligibilityRegion_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/EligibilityRegion.parquet")
    val HasGenre_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/HasGenre.parquet")
    val MakesPurchase_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/MakesPurchase.parquet")
    val Tag_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/Tag.parquet")
    val Includes_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/Includes.parquet")
    val Offers_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/Offers.parquet")
    val HasReview_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/HasReview.parquet")
    val FriendOf_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/FriendOf.parquet")
    val Actor_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/Actor.parquet")
    val PurchaseFor_DF = spark.read.format("parquet").load(path + "VHDFS/Parquet/PurchaseFor.parquet")


    println("PT Tables Read!!")

    //partition and save on HDFS
    if (partitionType.toLowerCase == "subject") {
      Retailer_DF.repartition(84, $"retailer").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/Retailer.parquet")
      Product_DF.repartition(84, $"product").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/Product.parquet")
      User_DF.repartition(84, $"user").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/User.parquet")
      Offer_DF.repartition(84, $"offer").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/Offer.parquet")
      City_DF.repartition(84, $"city").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/City.parquet")
      Review_DF.repartition(84, $"review").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/Review.parquet")
      Website_DF.repartition(84, $"website").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/Website.parquet")
      Role_DF.repartition(84, $"user").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/Role.parquet")
      Genre_DF.repartition(84, $"subgenre").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/Genre.parquet")
      Trailer_DF.repartition(84, $"product").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/Trailer.parquet")
      Purchase_DF.repartition(84, $"purchase").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/Purchase.parquet")
      Language_DF.repartition(84, $"product").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/Language.parquet")
      Likes_DF.repartition(84, $"user").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/Likes.parquet")
      Subscribes_DF.repartition(84, $"user").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/Subscribes.parquet")
      EligibilityRegion_DF.repartition(84, $"offer").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/EligibilityRegion.parquet")
      HasGenre_DF.repartition(84, $"product").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/HasGenre.parquet")
      MakesPurchase_DF.repartition(84, $"user").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/MakesPurchase.parquet")
      Tag_DF.repartition(84, $"product").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/Tag.parquet")
      Includes_DF.repartition(84, $"offer").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/Includes.parquet")
      Offers_DF.repartition(84, $"retailer").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/Offers.parquet")
      HasReview_DF.repartition(84, $"product").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/HasReview.parquet")
      FriendOf_DF.repartition(84, $"user1").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/FriendOf.parquet")
      Actor_DF.repartition(84, $"product").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/Actor.parquet")
      PurchaseFor_DF.repartition(84, $"purchase").write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Subject/Parquet/PurchaseFor.parquet")

      println("AVRO PT partitioned and saved! Subject based Partitioning!")

    }

    else if (partitionType == "horizontal") {

      Retailer_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/Retailer.parquet")
      Product_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/Product.parquet")
      User_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/User.parquet")
      Offer_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/Offer.parquet")
      City_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/City.parquet")
      Review_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/Review.parquet")
      Website_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/Website.parquet")
      Role_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/Role.parquet")
      Genre_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/Genre.parquet")
      Trailer_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/Trailer.parquet")
      Purchase_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/Purchase.parquet")
      Language_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/Language.parquet")
      Likes_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/Likes.parquet")
      Subscribes_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/Subscribes.parquet")
      EligibilityRegion_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/EligibilityRegion.parquet")
      HasGenre_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/HasGenre.parquet")
      MakesPurchase_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/MakesPurchase.parquet")
      Tag_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/Tag.parquet")
      Includes_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/Includes.parquet")
      Offers_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/Offers.parquet")
      HasReview_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/HasReview.parquet")
      FriendOf_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/FriendOf.parquet")
      Actor_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/Actor.parquet")
      PurchaseFor_DF.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Horizontal/Parquet/PurchaseFor.parquet")


      println("AVRO PT partitioned and saved! Horizontal partitioning!")
    }

    else if (partitionType.toLowerCase == "predicate") {

      val purchaseDate = Purchase_DF.select("purchase", "purchaseDate").toDF()
      val purchasePrice = Purchase_DF.select("purchase", "price").toDF()
      val purchasePurchaseFor = Purchase_DF.select("purchase", "purchaseFor").toDF()

      purchaseDate.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/purchaseDate.parquet")
      purchasePrice.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/purchasePrice.parquet")
      purchasePurchaseFor.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/purchasePurchaseFor.parquet")

      /*

      val reviewReviewer = Review_DF.select("review", "reviewer").toDF()
      val reviewRating = Review_DF.select("review", "rating").toDF()
      val reviewText = Review_DF.select("review", "text").toDF()
      val reviewTitle = Review_DF.select("review", "title").toDF()
      val reviewTotalVotes = Review_DF.select("review", "totalVotes").toDF()

      reviewReviewer.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/reviewReviewer.parquet")
      reviewRating.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/reviewRating.parquet")
      reviewText.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/reviewText.parquet")
      reviewTitle.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/reviewTitle.parquet")
      reviewTotalVotes.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/reviewTotalVotes.parquet")


      val SubgenreGenre = Genre_DF.select("subgenre", "genre").toDF()
      val SubgenreTopic = Genre_DF.select("subgenre", "topic").toDF()

      SubgenreGenre.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/SubgenreGenre.parquet")
      SubgenreTopic.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/SubgenreTopic.parquet")

      val offerValidThrough = Offer_DF.select("offer", "validThrough").toDF()
      val offerELigibleQuantity = Offer_DF.select("offer", "eligibleQuantity").toDF()
      val offerValidFrom = Offer_DF.select("offer", "validFrom").toDF()
      val offerPrice = Offer_DF.select("offer", "price").toDF()
      val offerSerialNumber = Offer_DF.select("offer", "serialNumber").toDF()
      val offerPriceValidUntil = Offer_DF.select("offer", "priceValidUntil").toDF()

      offerValidThrough.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/offerValidThrough.parquet")
      offerELigibleQuantity.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/offerELigibleQuantity.parquet")
      offerValidFrom.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/offerValidFrom.parquet")
      offerPrice.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/offerPrice.parquet")
      offerSerialNumber.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/offerSerialNumber.parquet")
      offerPriceValidUntil.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/offerPriceValidUntil.parquet")


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

      productProductCategory.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productProductCategory.parquet")
      productContentRating.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productContentRating.parquet")
      productTitle.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productTitle.parquet")
      productText.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productText.parquet")
      productDescription.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productDescription.parquet")
      productWordCount.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productWordCount.parquet")
      productPrintPage.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productPrintPage.parquet")
      productPublisher.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productPublisher.parquet")
      productPrintSection.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productPrintSection.parquet")
      productCaption.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productCaption.parquet")
      productComposer.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productComposer.parquet")
      productOpus.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productOpus.parquet")
      productExpires.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productExpires.parquet")
      productMovement.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productMovement.parquet")
      productConductor.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productConductor.parquet")
      productPerformer.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productPerformer.parquet")
      productHomepage.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productHomepage.parquet")
      productKeywords.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productKeywords.parquet")
      productProducer.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productProducer.parquet")
      productDuration.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productDuration.parquet")
      productAward.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productAward.parquet")
      productPerformed_in.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productPerformed_in.parquet")
      productContentSize.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productContentSize.parquet")
      productBookEdition.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productBookEdition.parquet")
      productIsbn.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productIsbn.parquet")
      productRelease.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productRelease.parquet")
      productArtist.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productArtist.parquet")
      productDatePublished.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productDatePublished.parquet")
      productRecord_number.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productRecord_number.parquet")
      productPrintEdition.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productPrintEdition.parquet")
      productPrintColumn.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productPrintColumn.parquet")
      productNumberOfPages.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/productNumberOfPages.parquet")


      val websiteLanguage = Website_DF.select("website", "language").toDF()
      val websiteHits = Website_DF.select("website", "hits").toDF()
      val websiteUrl = Website_DF.select("website", "url").toDF()

      websiteLanguage.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/websiteLanguage.parquet")
      websiteHits.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/websiteHits.parquet")
      websiteUrl.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/websiteUrl.parquet")

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

      userUserId.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/userUserId.parquet")
      userGivenName.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/userGivenName.parquet")
      userFamilyName.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/userFamilyName.parquet")
      userEmail.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/userEmail.parquet")
      userLocation.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/userLocation.parquet")
      userGender.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/userGender.parquet")
      userBirthDate.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/userBirthDate.parquet")
      userAge.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/userAge.parquet")
      userNationality.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/userNationality.parquet")
      userTelephone.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/userTelephone.parquet")
      userHomepage.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/userHomepage.parquet")
      userJobTitle.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/userJobTitle.parquet")


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

      retailerName.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/retailerName.parquet")
      retailerLegalName.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/retailerLegalName.parquet")
      retailerOpeningHours.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/retailerOpeningHours.parquet")
      retailerDescription.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/retailerDescription.parquet")
      retailerContactPoint.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/retailerContactPoint.parquet")
      retailerTelephone.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/retailerTelephone.parquet")
      retailerEmail.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/retailerEmail.parquet")
      retailerPaymentAccepted.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/retailerPaymentAccepted.parquet")
      retailerFaxNumber.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/retailerFaxNumber.parquet")
      retailerAggregateRating.repartition(84).write.option("header", "true").format("parquet").mode(SaveMode.Overwrite).save(path + "Predicate/Parquet/retailerAggregateRating.parquet")

       */


      println("Parquet PT partitioned and saved! Predicate based partitioning!")

    }


  }
}
