package ee.ut.cs.bigdata.watdiv.querying.csv

import ee.ut.cs.bigdata.watdiv.querying.queries.PTQueries
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, FileOutputStream}

object PropertyTablesQP {
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
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    var partitionType = args(1) // value = {"Horizontal", "Subject", or "Predicate"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/PT/"

    println("PT Querying!")


    if (partitionType == "Predicate") {


      /*
      FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(s"$path/$partitionType/CSV")).groupBy(file => file.getPath().getName().split("(?=\\p{Upper})")(0)).foreach {

        filestatus =>
          filestatus._2.foreach {
            f =>
              val ptTable = spark.read.option("header", true).csv(f.getPath().toString)
              println(ptTable.count())
          }
      }

       */


      //Purchase

      val purchaseProp1 = spark.read.option("header", true).csv(path + "Predicate/CSV/purchaseDate.csv")
      val purchaseProp2 = spark.read.option("header", true).csv(path + "Predicate/CSV/purchasePrice.csv")
      val purchaseProp3 = spark.read.option("header", true).csv(path + "Predicate/CSV/purchasePurchaseFor.csv")


      val purchase_join1 = purchaseProp1.join(purchaseProp2, purchaseProp1("purchase") === purchaseProp2("purchase")).drop(purchaseProp2("purchase"))
      val purchase_join2 = purchase_join1.join(purchaseProp3, purchase_join1("purchase") === purchaseProp3("purchase")).drop(purchaseProp3("purchase"))

      purchase_join2.createOrReplaceTempView("Purchase")


      // Subgenre

      val subgenreProp1 = spark.read.option("header", true).csv(path + "Predicate/CSV/SubgenreGenre.csv")
      val subgenreProp2 = spark.read.option("header", true).csv(path + "Predicate/CSV/SubgenreTopic.csv")

      val subgenre_join1 = subgenreProp1.join(subgenreProp2, subgenreProp1("subgenre") === subgenreProp2("subgenre")).drop(subgenreProp2("subgenre")).distinct()

      subgenre_join1.createOrReplaceTempView("Genre")

      //Retailer
      val retailerProp1 = spark.read.option("header", true).csv(path + "Predicate/CSV/retailerName.csv")
      val retailerProp2 = spark.read.option("header", true).csv(path + "Predicate/CSV/retailerLegalName.csv")
      val retailerProp3 = spark.read.option("header", true).csv(path + "Predicate/CSV/retailerOpeningHours.csv")
      val retailerProp4 = spark.read.option("header", true).csv(path + "Predicate/CSV/retailerDescription.csv")
      val retailerProp5 = spark.read.option("header", true).csv(path + "Predicate/CSV/retailerContactPoint.csv")
      val retailerProp6 = spark.read.option("header", true).csv(path + "Predicate/CSV/retailerTelephone.csv")
      val retailerProp7 = spark.read.option("header", true).csv(path + "Predicate/CSV/retailerEmail.csv")
      val retailerProp8 = spark.read.option("header", true).csv(path + "Predicate/CSV/retailerPaymentAccepted.csv")
      val retailerProp9 = spark.read.option("header", true).csv(path + "Predicate/CSV/retailerFaxNumber.csv")
      val retailerProp10 = spark.read.option("header", true).csv(path + "Predicate/CSV/retailerAggregateRating.csv")

      val retailer_join1 = retailerProp1.join(retailerProp2, retailerProp1("retailer") === retailerProp2("retailer")).drop(retailerProp2("retailer"))
      val retailer_join2 = retailer_join1.join(retailerProp3, retailer_join1("retailer") === retailerProp3("retailer")).drop(retailerProp3("retailer"))
      val retailer_join3 = retailer_join2.join(retailerProp4, retailer_join2("retailer") === retailerProp4("retailer")).drop(retailerProp4("retailer"))
      val retailer_join4 = retailer_join3.join(retailerProp5, retailer_join3("retailer") === retailerProp5("retailer")).drop(retailerProp5("retailer"))
      val retailer_join5 = retailer_join4.join(retailerProp6, retailer_join4("retailer") === retailerProp6("retailer")).drop(retailerProp6("retailer"))
      val retailer_join6 = retailer_join5.join(retailerProp7, retailer_join5("retailer") === retailerProp7("retailer")).drop(retailerProp7("retailer"))
      val retailer_join7 = retailer_join6.join(retailerProp8, retailer_join6("retailer") === retailerProp8("retailer")).drop(retailerProp8("retailer"))
      val retailer_join8 = retailer_join7.join(retailerProp9, retailer_join7("retailer") === retailerProp9("retailer")).drop(retailerProp9("retailer"))
      val retailer_join9 = retailer_join8.join(retailerProp10, retailer_join8("retailer") === retailerProp10("retailer")).drop(retailerProp10("retailer"))

      retailer_join9.createOrReplaceTempView("Retailer")

      // Offer
      val offerProp1 = spark.read.option("header", true).csv(path + "Predicate/CSV/offerValidThrough.csv")
      val offerProp2 = spark.read.option("header", true).csv(path + "Predicate/CSV/offerELigibleQuantity.csv")
      val offerProp3 = spark.read.option("header", true).csv(path + "Predicate/CSV/offerValidFrom.csv")
      val offerProp4 = spark.read.option("header", true).csv(path + "Predicate/CSV/offerPrice.csv")
      val offerProp5 = spark.read.option("header", true).csv(path + "Predicate/CSV/offerSerialNumber.csv")
      val offerProp6 = spark.read.option("header", true).csv(path + "Predicate/CSV/offerPriceValidUntil.csv")

      val offer_join1 = offerProp1.join(offerProp2, offerProp1("offer") === offerProp2("offer")).drop(offerProp2("offer"))
      val offer_join2 = offer_join1.join(offerProp3, offer_join1("offer") === offerProp3("offer")).drop(offerProp3("offer"))
      val offer_join3 = offer_join2.join(offerProp4, offer_join2("offer") === offerProp4("offer")).drop(offerProp4("offer"))
      val offer_join4 = offer_join3.join(offerProp5, offer_join3("offer") === offerProp5("offer")).drop(offerProp5("offer"))
      val offer_join5 = offer_join4.join(offerProp6, offer_join4("offer") === offerProp6("offer")).drop(offerProp6("offer"))

      offer_join5.createOrReplaceTempView("Offer")

      //Review
      val reviewProp1 = spark.read.option("header", true).csv(path + "Predicate/CSV/reviewReviewer.csv")
      val reviewProp2 = spark.read.option("header", true).csv(path + "Predicate/CSV/reviewRating.csv")
      val reviewProp3 = spark.read.option("header", true).csv(path + "Predicate/CSV/reviewText.csv")
      val reviewProp4 = spark.read.option("header", true).csv(path + "Predicate/CSV/reviewTitle.csv")
      val reviewProp5 = spark.read.option("header", true).csv(path + "Predicate/CSV/reviewTotalVotes.csv")

      val review_join1 = reviewProp1.join(reviewProp2, reviewProp1("review") === reviewProp2("review")).drop(reviewProp2("review"))
      val review_join2 = review_join1.join(reviewProp3, review_join1("review") === reviewProp3("review")).drop(reviewProp3("review"))
      val review_join3 = review_join2.join(reviewProp4, review_join2("review") === reviewProp4("review")).drop(reviewProp4("review"))
      val review_join4 = review_join3.join(reviewProp5, review_join3("review") === reviewProp5("review")).drop(reviewProp5("review"))

      review_join4.createOrReplaceTempView("Review")


      // Product
      val productProp1 = spark.read.option("header", true).csv(path + "Predicate/CSV/productProductCategory.csv")
      val productProp2 = spark.read.option("header", true).csv(path + "Predicate/CSV/productContentRating.csv")
      val productProp3 = spark.read.option("header", true).csv(path + "Predicate/CSV/productTitle.csv")
      val productProp4 = spark.read.option("header", true).csv(path + "Predicate/CSV/productText.csv")
      val productProp5 = spark.read.option("header", true).csv(path + "Predicate/CSV/productDescription.csv")
      val productProp6 = spark.read.option("header", true).csv(path + "Predicate/CSV/productWordCount.csv")
      val productProp7 = spark.read.option("header", true).csv(path + "Predicate/CSV/productPrintPage.csv")
      val productProp8 = spark.read.option("header", true).csv(path + "Predicate/CSV/productPublisher.csv")
      val productProp9 = spark.read.option("header", true).csv(path + "Predicate/CSV/productPrintSection.csv")
      val productProp10 = spark.read.option("header", true).csv(path + "Predicate/CSV/productCaption.csv")
      val productProp11 = spark.read.option("header", true).csv(path + "Predicate/CSV/productComposer.csv")
      val productProp12 = spark.read.option("header", true).csv(path + "Predicate/CSV/productOpus.csv")
      val productProp13 = spark.read.option("header", true).csv(path + "Predicate/CSV/productExpires.csv")
      val productProp14 = spark.read.option("header", true).csv(path + "Predicate/CSV/productMovement.csv")
      val productProp15 = spark.read.option("header", true).csv(path + "Predicate/CSV/productConductor.csv")
      val productProp16 = spark.read.option("header", true).csv(path + "Predicate/CSV/productPerformer.csv")
      val productProp17 = spark.read.option("header", true).csv(path + "Predicate/CSV/productHomepage.csv")
      val productProp18 = spark.read.option("header", true).csv(path + "Predicate/CSV/productKeywords.csv")
      val productProp19 = spark.read.option("header", true).csv(path + "Predicate/CSV/productProducer.csv")
      val productProp20 = spark.read.option("header", true).csv(path + "Predicate/CSV/productDuration.csv")
      val productProp21 = spark.read.option("header", true).csv(path + "Predicate/CSV/productAward.csv")
      val productProp22 = spark.read.option("header", true).csv(path + "Predicate/CSV/productPerformed_in.csv")
      val productProp23 = spark.read.option("header", true).csv(path + "Predicate/CSV/productContentSize.csv")
      val productProp24 = spark.read.option("header", true).csv(path + "Predicate/CSV/productBookEdition.csv")
      val productProp25 = spark.read.option("header", true).csv(path + "Predicate/CSV/productIsbn.csv")
      val productProp26 = spark.read.option("header", true).csv(path + "Predicate/CSV/productRelease.csv")
      val productProp27 = spark.read.option("header", true).csv(path + "Predicate/CSV/productArtist.csv")
      val productProp28 = spark.read.option("header", true).csv(path + "Predicate/CSV/productDatePublished.csv")
      val productProp29 = spark.read.option("header", true).csv(path + "Predicate/CSV/productRecord_number.csv")
      val productProp30 = spark.read.option("header", true).csv(path + "Predicate/CSV/productPrintEdition.csv")
      val productProp31 = spark.read.option("header", true).csv(path + "Predicate/CSV/productPrintColumn.csv")
      val productProp32 = spark.read.option("header", true).csv(path + "Predicate/CSV/productNumberOfPages.csv")

      val product_join1 = productProp1.distinct().join(productProp2.distinct(), productProp1("product") === productProp2("product")).drop(productProp2("product"))
      val product_join2 = product_join1.distinct().join(productProp3, product_join1("product") === productProp3("product")).drop(productProp3("product"))
      val product_join3 = product_join2.distinct().join(productProp4, product_join2("product") === productProp4("product")).drop(productProp4("product"))
      val product_join4 = product_join3.distinct().join(productProp5, product_join3("product") === productProp5("product")).drop(productProp5("product"))
      val product_join5 = product_join4.distinct().join(productProp6, product_join4("product") === productProp6("product")).drop(productProp6("product"))
      val product_join6 = product_join5.distinct().join(productProp7, product_join5("product") === productProp7("product")).drop(productProp7("product"))
      val product_join7 = product_join6.distinct().join(productProp8, product_join6("product") === productProp8("product")).drop(productProp8("product"))
      val product_join8 = product_join7.distinct().join(productProp9, product_join7("product") === productProp9("product")).drop(productProp9("product"))
      val product_join9 = product_join8.distinct().join(productProp10, product_join8("product") === productProp10("product")).drop(productProp10("product"))
      val product_join10 = product_join9.distinct().join(productProp11, product_join9("product") === productProp11("product")).drop(productProp11("product"))
      val product_join11 = product_join10.distinct().join(productProp12, product_join10("product") === productProp12("product")).drop(productProp12("product"))
      val product_join12 = product_join11.distinct().join(productProp13, product_join11("product") === productProp13("product")).drop(productProp13("product"))
      val product_join13 = product_join12.distinct().join(productProp14, product_join12("product") === productProp14("product")).drop(productProp14("product"))
      val product_join14 = product_join13.distinct().join(productProp15, product_join13("product") === productProp15("product")).drop(productProp15("product"))
      val product_join15 = product_join14.distinct().join(productProp16, product_join14("product") === productProp16("product")).drop(productProp16("product"))
      val product_join16 = product_join15.distinct().join(productProp17, product_join15("product") === productProp17("product")).drop(productProp17("product"))
      val product_join17 = product_join16.distinct().join(productProp18, product_join16("product") === productProp18("product")).drop(productProp18("product"))
      val product_join18 = product_join17.distinct().join(productProp19, product_join17("product") === productProp19("product")).drop(productProp19("product"))
      val product_join19 = product_join18.distinct().join(productProp20, product_join18("product") === productProp20("product")).drop(productProp20("product"))
      val product_join20 = product_join19.distinct().join(productProp21, product_join19("product") === productProp21("product")).drop(productProp21("product"))
      val product_join21 = product_join20.distinct().join(productProp22, product_join20("product") === productProp22("product")).drop(productProp22("product"))
      val product_join22 = product_join21.distinct().join(productProp23, product_join21("product") === productProp23("product")).drop(productProp23("product"))
      val product_join23 = product_join22.distinct().join(productProp24, product_join22("product") === productProp24("product")).drop(productProp24("product"))
      val product_join24 = product_join23.distinct().join(productProp25, product_join23("product") === productProp25("product")).drop(productProp25("product"))
      val product_join25 = product_join24.distinct().join(productProp26, product_join24("product") === productProp26("product")).drop(productProp26("product"))
      val product_join26 = product_join25.distinct().join(productProp27, product_join25("product") === productProp27("product")).drop(productProp27("product"))
      val product_join27 = product_join26.distinct().join(productProp28, product_join26("product") === productProp28("product")).drop(productProp28("product"))
      val product_join28 = product_join27.distinct().join(productProp29, product_join27("product") === productProp29("product")).drop(productProp29("product"))
      val product_join29 = product_join28.distinct().join(productProp30, product_join28("product") === productProp30("product")).drop(productProp30("product"))
      val product_join30 = product_join29.distinct().join(productProp31, product_join29("product") === productProp31("product")).drop(productProp31("product"))
      val product_join31 = product_join30.distinct().join(productProp32, product_join30("product") === productProp32("product")).drop(productProp32("product")).distinct()

      product_join31.createOrReplaceTempView("Product")

      //User
      val userProp1 = spark.read.option("header", true).csv(path + "Predicate/CSV/userUserId.csv")
      val userProp2 = spark.read.option("header", true).csv(path + "Predicate/CSV/userGivenName.csv")
      val userProp3 = spark.read.option("header", true).csv(path + "Predicate/CSV/userFamilyName.csv")
      val userProp4 = spark.read.option("header", true).csv(path + "Predicate/CSV/userEmail.csv")
      val userProp5 = spark.read.option("header", true).csv(path + "Predicate/CSV/userLocation.csv")
      val userProp6 = spark.read.option("header", true).csv(path + "Predicate/CSV/userGender.csv")
      val userProp7 = spark.read.option("header", true).csv(path + "Predicate/CSV/userBirthDate.csv")
      val userProp8 = spark.read.option("header", true).csv(path + "Predicate/CSV/userAge.csv")
      val userProp9 = spark.read.option("header", true).csv(path + "Predicate/CSV/userNationality.csv")
      val userProp10 = spark.read.option("header", true).csv(path + "Predicate/CSV/userTelephone.csv")
      val userProp11 = spark.read.option("header", true).csv(path + "Predicate/CSV/userHomepage.csv")
      val userProp12 = spark.read.option("header", true).csv(path + "Predicate/CSV/userJobTitle.csv")

      val user_join1 = userProp1.join(userProp2, userProp1("user") === userProp2("user")).drop(userProp2("user"))
      val user_join2 = user_join1.join(userProp3, user_join1("user") === userProp3("user")).drop(userProp3("user"))
      val user_join3 = user_join2.join(userProp4, user_join2("user") === userProp4("user")).drop(userProp4("user"))
      val user_join4 = user_join3.join(userProp5, user_join3("user") === userProp5("user")).drop(userProp5("user"))
      val user_join5 = user_join4.join(userProp6, user_join4("user") === userProp6("user")).drop(userProp6("user"))
      val user_join6 = user_join5.join(userProp7, user_join5("user") === userProp7("user")).drop(userProp7("user"))
      val user_join7 = user_join6.join(userProp8, user_join6("user") === userProp8("user")).drop(userProp8("user"))
      val user_join8 = user_join7.join(userProp9, user_join7("user") === userProp9("user")).drop(userProp9("user"))
      val user_join9 = user_join8.join(userProp10, user_join8("user") === userProp10("user")).drop(userProp10("user"))
      val user_join10 = user_join9.join(userProp11, user_join9("user") === userProp11("user")).drop(userProp11("user"))
      val user_join11 = user_join10.join(userProp12, user_join10("user") === userProp12("user")).drop(userProp12("user"))

      user_join11.createOrReplaceTempView("User")

      //Website
      val WebsitePro1 = spark.read.option("header", true).csv(path + "Predicate/CSV/websiteLanguage.csv").toDF()
      val WebsitePro2 = spark.read.option("header", true).csv(path + "Predicate/CSV/websiteHits.csv").toDF()
      val WebsitePro3 = spark.read.option("header", true).csv(path + "Predicate/CSV/websiteUrl.csv").toDF()

      val website_join1 = WebsitePro1.join(WebsitePro2, WebsitePro1("website") === WebsitePro2("website")).drop(WebsitePro2("website"))
      val website_join2 = website_join1.join(WebsitePro3, website_join1("website") === WebsitePro3("website")).drop(WebsitePro3("website"))

      website_join2.createOrReplaceTempView("Website")


      val City_DF = spark.read.option("header", true).csv(path + "Horizontal/CSV/City.csv")
      val Role_DF = spark.read.option("header", true).csv(path + "Horizontal/CSV/Role.csv")
      val Trailer_DF = spark.read.option("header", true).csv(path + "Horizontal/CSV/Trailer.csv")
      val Language_DF = spark.read.option("header", true).csv(path + "Horizontal/CSV/Language.csv")
      val Likes_DF = spark.read.option("header", true).csv(path + "Horizontal/CSV/Likes.csv")
      val Subscribes_DF = spark.read.option("header", true).csv(path + "Horizontal/CSV/Subscribes.csv")
      val EligibilityRegion_DF = spark.read.option("header", true).csv(path + "Horizontal/CSV/EligibilityRegion.csv")
      val HasGenre_DF = spark.read.option("header", true).csv(path + "Horizontal/CSV/HasGenre.csv")
      val MakesPurchase_DF = spark.read.option("header", true).csv(path + "Horizontal/CSV/MakesPurchase.csv")
      val Tag_DF = spark.read.option("header", true).csv(path + "Horizontal/CSV/Tag.csv")
      val Includes_DF = spark.read.option("header", true).csv(path + "Horizontal/CSV/Includes.csv")
      val Offers_DF = spark.read.option("header", true).csv(path + "Horizontal/CSV/Offers.csv")
      val HasReview_DF = spark.read.option("header", true).csv(path + "Horizontal/CSV/HasReview.csv")
      val FriendOf_DF = spark.read.option("header", true).csv(path + "Horizontal/CSV/FriendOf.csv")
      val Actor_DF = spark.read.option("header", true).csv(path + "Horizontal/CSV/Actor.csv")
      val PurchaseFor_DF = spark.read.option("header", true).csv(path + "Horizontal/CSV/PurchaseFor.csv")


      City_DF.createOrReplaceTempView("City")
      Role_DF.createOrReplaceTempView("Role")
      Trailer_DF.createOrReplaceTempView("Trailer")
      Actor_DF.createOrReplaceTempView("Actor")
      Language_DF.createOrReplaceTempView("Language")
      Likes_DF.createOrReplaceTempView("Likes")
      Subscribes_DF.createOrReplaceTempView("Subscribes")
      EligibilityRegion_DF.createOrReplaceTempView("EligibilityRegion")
      HasGenre_DF.createOrReplaceTempView("HasGenre")
      MakesPurchase_DF.createOrReplaceTempView("MakesPurchase")
      Tag_DF.createOrReplaceTempView("Tag")
      Includes_DF.createOrReplaceTempView("Includes")
      Offers_DF.createOrReplaceTempView("Offers")
      HasReview_DF.createOrReplaceTempView("HasReview")
      FriendOf_DF.createOrReplaceTempView("FriendOf")
      PurchaseFor_DF.createOrReplaceTempView("PurchaseFor")


    }

    else {
      FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(s"$path/$partitionType/CSV")).foreach {
        x =>
          val ptTable = spark.read.option("header", true).csv(x.getPath().toString)
          ptTable.createOrReplaceTempView(x.getPath().getName().substring(0, x.getPath().getName().lastIndexOf('.')))
      }
    }


    //create file to write the query run time results    
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/csv/PT/$ds.txt"), true)

    val queries = List(
      new PTQueries c1,
      new PTQueries c2,
      new PTQueries c3,
      new PTQueries f1,
      new PTQueries f2,
      new PTQueries f3,
      new PTQueries f4,
      new PTQueries f5,
      new PTQueries l1,
      new PTQueries l2,
      new PTQueries l3,
      new PTQueries l4,
      new PTQueries l5,
      new PTQueries s1,
      new PTQueries s2,
      new PTQueries s3,
      new PTQueries s4,
      new PTQueries s5,
      new PTQueries s6,
      new PTQueries s7)

    var count = 1
    for (query <- queries) {
      //run query and calculate the run time
      val startTime = System.nanoTime()
      val df_count = spark.sql(query).count()
      println(df_count)
      //df.take(100).foreach(println)
      val endTime = System.nanoTime()
      val result = (endTime - startTime).toDouble / 1000000000

      //write the result into the log file
      if (count != queries.size) {
        Console.withOut(fos) {
          print(result + ",")
        }
      } else {
        Console.withOut(fos) {
          println(result)
        }
      }
      count += 1
    }
    println("All Queries are Done - CSV - PT!")

  }
}
