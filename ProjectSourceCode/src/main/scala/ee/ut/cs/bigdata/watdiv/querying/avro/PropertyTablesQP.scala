package ee.ut.cs.bigdata.watdiv.querying.avro

import ee.ut.cs.bigdata.watdiv.querying.queries.PTQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}

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
      .appName("RDFBench Avro PT")
      .getOrCreate()
    val ds = args(0) //value = {"100M", "500M, or "1B"}
    var partitionType = args(1) // value = {"Horizontal", "Subject", or "Predicate"}
    val path = s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/WATDIV/$ds/PT/"

    println("PT Querying!")


    if (partitionType == "Predicate") {


      FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(s"$path/$partitionType/Avro")).groupBy(file=>file.getPath().getName().split("(?=\\p{Upper})")(0)).foreach {

//        file =>
//          println(file.getPath().getName().split("(?=\\p{Upper})")(0))
//


//          val ptTable = spark.read.format("avro").load(file.getPath().toString)
//          ptTable.createOrReplaceTempView(file.getPath.getName.split("(?=\\p{Upper})")(0))

        x=>x._2.foreach{
          f=>println(f.getPath.getName)
        }
        println()

      }





      /*
      //Purchase

      val purchaseProp1 = spark.read.format("avro").load(path + "Predicate/Avro/purchaseDate.avro")
      val purchaseProp2 = spark.read.format("avro").load(path + "Predicate/Avro/purchasePrice.avro")
      val purchaseProp3 = spark.read.format("avro").load(path + "Predicate/Avro/purchasePurchaseFor.avro")


      val purchase_join1 = purchaseProp1.join(purchaseProp2, purchaseProp1("purchase") === purchaseProp2("purchase")).drop(purchaseProp2("purchase"))
      val purchase_join2 = purchase_join1.join(purchaseProp3, purchase_join1("purchase") === purchaseProp3("purchase")).drop(purchaseProp3("purchase"))


      // Subgenre

      val subgenreProp1 = spark.read.format("avro").load(path + "Predicate/Avro/SubgenreGenre.avro")
      val subgenreProp2 = spark.read.format("avro").load(path + "Predicate/Avro/SubgenreTopic.avro")

      val subgenre_join1 = subgenreProp1.join(subgenreProp2, subgenreProp1("subgenre") === subgenreProp2("subgenre")).drop(subgenreProp2("subgenre")).distinct()


      //Retailer

      val retailerProp1 = spark.read.format("avro").load(path + "Predicate/Avro/retailerName.avro")
      val retailerProp2 = spark.read.format("avro").load(path + "Predicate/Avro/retailerLegalName.avro")
      val retailerProp3 = spark.read.format("avro").load(path + "Predicate/Avro/retailerOpeningHours.avro")
      val retailerProp4 = spark.read.format("avro").load(path + "Predicate/Avro/retailerDescription.avro")
      val retailerProp5 = spark.read.format("avro").load(path + "Predicate/Avro/retailerContactPoint.avro")
      val retailerProp6 = spark.read.format("avro").load(path + "Predicate/Avro/retailerTelephone.avro")
      val retailerProp7 = spark.read.format("avro").load(path + "Predicate/Avro/retailerEmail.avro")
      val retailerProp8 = spark.read.format("avro").load(path + "Predicate/Avro/retailerPaymentAccepted.avro")
      val retailerProp9 = spark.read.format("avro").load(path + "Predicate/Avro/retailerFaxNumber.avro")
      val retailerProp10 = spark.read.format("avro").load(path + "Predicate/Avro/retailerAggregateRating.avro")


      val retailer_join1 = retailerProp1.join(retailerProp2, retailerProp1("retailer") === retailerProp2("retailer")).drop(retailerProp2("retailer"))
      val retailer_join2 = retailer_join1.join(retailerProp3, retailer_join1("retailer") === retailerProp3("retailer")).drop(retailerProp3("retailer"))
      val retailer_join3 = retailer_join2.join(retailerProp4, retailer_join2("retailer") === retailerProp4("retailer")).drop(retailerProp4("retailer"))
      val retailer_join4 = retailer_join3.join(retailerProp5, retailer_join3("retailer") === retailerProp5("retailer")).drop(retailerProp5("retailer"))
      val retailer_join5 = retailer_join4.join(retailerProp6, retailer_join4("retailer") === retailerProp6("retailer")).drop(retailerProp6("retailer"))
      val retailer_join6 = retailer_join5.join(retailerProp7, retailer_join5("retailer") === retailerProp7("retailer")).drop(retailerProp7("retailer"))
      val retailer_join7 = retailer_join6.join(retailerProp8, retailer_join6("retailer") === retailerProp8("retailer")).drop(retailerProp8("retailer"))
      val retailer_join8 = retailer_join7.join(retailerProp9, retailer_join7("retailer") === retailerProp9("retailer")).drop(retailerProp9("retailer"))
      val retailer_join9 = retailer_join8.join(retailerProp10, retailer_join8("retailer") === retailerProp10("retailer")).drop(retailerProp10("retailer"))


      // Offer

      val offerProp1 = spark.read.format("avro").load(path + "Predicate/Avro/offerValidThrough.avro")
      val offerProp2 = spark.read.format("avro").load(path + "Predicate/Avro/offerELigibleQuantity.avro")
      val offerProp3 = spark.read.format("avro").load(path + "Predicate/Avro/offerValidFrom.avro")
      val offerProp4 = spark.read.format("avro").load(path + "Predicate/Avro/offerPrice.avro")
      val offerProp5 = spark.read.format("avro").load(path + "Predicate/Avro/offerSerialNumber.avro")
      val offerProp6 = spark.read.format("avro").load(path + "Predicate/Avro/offerPriceValidUntil.avro")


      val offer_join1 = offerProp1.join(offerProp2, offerProp1("offer") === offerProp2("offer")).drop(offerProp2("offer"))
      val offer_join2 = offer_join1.join(offerProp3, offer_join1("offer") === offerProp3("offer")).drop(offerProp3("offer"))
      val offer_join3 = offer_join2.join(offerProp4, offer_join2("offer") === offerProp4("offer")).drop(offerProp4("offer"))
      val offer_join4 = offer_join3.join(offerProp5, offer_join3("offer") === offerProp5("offer")).drop(offerProp5("offer"))
      val offer_join5 = offer_join4.join(offerProp6, offer_join4("offer") === offerProp6("offer")).drop(offerProp6("offer"))



      //Review

      val reviewProp1 = spark.read.format("avro").load(path + "Predicate/Avro/reviewReviewer.avro")
      val reviewProp2 = spark.read.format("avro").load(path + "Predicate/Avro/reviewRating.avro")
      val reviewProp3 = spark.read.format("avro").load(path + "Predicate/Avro/reviewText.avro")
      val reviewProp4 = spark.read.format("avro").load(path + "Predicate/Avro/reviewTitle.avro")
      val reviewProp5 = spark.read.format("avro").load(path + "Predicate/Avro/reviewTotalVotes.avro")

      val review_join1 = reviewProp1.join(reviewProp2, reviewProp1("review") === reviewProp2("review")).drop(reviewProp2("review"))
      val review_join2 = review_join1.join(reviewProp3, review_join1("review") === reviewProp3("review")).drop(reviewProp3("review"))
      val review_join3 = review_join2.join(reviewProp4, review_join2("review") === reviewProp4("review")).drop(reviewProp4("review"))
      val review_join4 = review_join3.join(reviewProp5, review_join3("review") === reviewProp5("review")).drop(reviewProp5("review"))


      // Product

      val productProp1 = spark.read.format("avro").load(path + "Predicate/Avro/productProductCategory.avro")
      val productProp2 = spark.read.format("avro").load(path + "Predicate/Avro/productContentRating.avro")
      val productProp3 = spark.read.format("avro").load(path + "Predicate/Avro/productTitle.avro")
      val productProp4 = spark.read.format("avro").load(path + "Predicate/Avro/productText.avro")
      val productProp5 = spark.read.format("avro").load(path + "Predicate/Avro/productDescription.avro")
      val productProp6 = spark.read.format("avro").load(path + "Predicate/Avro/productWordCount.avro")
      val productProp7 = spark.read.format("avro").load(path + "Predicate/Avro/productPrintPage.avro")
      val productProp8 = spark.read.format("avro").load(path + "Predicate/Avro/productPublisher.avro")
      val productProp9 = spark.read.format("avro").load(path + "Predicate/Avro/productPrintSection.avro")
      val productProp10 = spark.read.format("avro").load(path + "Predicate/Avro/productCaption.avro")
      val productProp11 = spark.read.format("avro").load(path + "Predicate/Avro/productComposer.avro")
      val productProp12 = spark.read.format("avro").load(path + "Predicate/Avro/productOpus.avro")
      val productProp13 = spark.read.format("avro").load(path + "Predicate/Avro/productExpires.avro")
      val productProp14 = spark.read.format("avro").load(path + "Predicate/Avro/productMovement.avro")
      val productProp15 = spark.read.format("avro").load(path + "Predicate/Avro/productConductor.avro")
      val productProp16 = spark.read.format("avro").load(path + "Predicate/Avro/productPerformer.avro")
      val productProp17 = spark.read.format("avro").load(path + "Predicate/Avro/productHomepage.avro")
      val productProp18 = spark.read.format("avro").load(path + "Predicate/Avro/productKeywords.avro")
      val productProp19 = spark.read.format("avro").load(path + "Predicate/Avro/productProducer.avro")
      val productProp20 = spark.read.format("avro").load(path + "Predicate/Avro/productDuration.avro")
      val productProp21 = spark.read.format("avro").load(path + "Predicate/Avro/productAward.avro")
      val productProp22 = spark.read.format("avro").load(path + "Predicate/Avro/productPerformed_in.avro")
      val productProp23 = spark.read.format("avro").load(path + "Predicate/Avro/productContentSize.avro")
      val productProp24 = spark.read.format("avro").load(path + "Predicate/Avro/productBookEdition.avro")
      val productProp25 = spark.read.format("avro").load(path + "Predicate/Avro/productIsbn.avro")
      val productProp26 = spark.read.format("avro").load(path + "Predicate/Avro/productRelease.avro")
      val productProp27 = spark.read.format("avro").load(path + "Predicate/Avro/productArtist.avro")
      val productProp28 = spark.read.format("avro").load(path + "Predicate/Avro/productDatePublished.avro")
      val productProp29 = spark.read.format("avro").load(path + "Predicate/Avro/productRecord_number.avro")
      val productProp30 = spark.read.format("avro").load(path + "Predicate/Avro/productPrintEdition.avro")
      val productProp31 = spark.read.format("avro").load(path + "Predicate/Avro/productPrintColumn.avro")
      val productProp32 = spark.read.format("avro").load(path + "Predicate/Avro/productNumberOfPages.avro")

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




      //User

      val userProp1 = spark.read.format("avro").load(path + "Predicate/Avro/userUserId.avro")
      val userProp2 = spark.read.format("avro").load(path + "Predicate/Avro/userGivenName.avro")
      val userProp3 = spark.read.format("avro").load(path + "Predicate/Avro/userFamilyName.avro")
      val userProp4 = spark.read.format("avro").load(path + "Predicate/Avro/userEmail.avro")
      val userProp5 = spark.read.format("avro").load(path + "Predicate/Avro/userLocation.avro")
      val userProp6 = spark.read.format("avro").load(path + "Predicate/Avro/userGender.avro")
      val userProp7 = spark.read.format("avro").load(path + "Predicate/Avro/userBirthDate.avro")
      val userProp8 = spark.read.format("avro").load(path + "Predicate/Avro/userAge.avro")
      val userProp9 = spark.read.format("avro").load(path + "Predicate/Avro/userNationality.avro")
      val userProp10 = spark.read.format("avro").load(path + "Predicate/Avro/userTelephone.avro")
      val userProp11 = spark.read.format("avro").load(path + "Predicate/Avro/userHomepage.avro")
      val userProp12 = spark.read.format("avro").load(path + "Predicate/Avro/userJobTitle.avro")

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



      //Website

      val WebsitePro1 = spark.read.format("avro").load(path + "Predicate/Avro/websiteLanguage.avro").toDF()
      val WebsitePro2 = spark.read.format("avro").load(path + "Predicate/Avro/websiteHits.avro").toDF()
      val WebsitePro3 = spark.read.format("avro").load(path + "Predicate/Avro/websiteUrl.avro").toDF()

      val website_join1 = WebsitePro1.join(WebsitePro2, WebsitePro1("website") === WebsitePro2("website")).drop(WebsitePro2("website"))
      val website_join2 = website_join1.join(WebsitePro3, website_join1("website") === WebsitePro3("website")).drop(WebsitePro3("website"))

       */




    }

    else {
      FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(s"$path/$partitionType/Avro")).foreach {
        x =>
          val ptTable = spark.read.format("avro").load(x.getPath().toString)
          ptTable.createOrReplaceTempView(x.getPath().getName().substring(0, x.getPath().getName().lastIndexOf('.')))
      }
    }


    //create file to write the query run time results    
    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/$ds/avro/PT/$ds.txt"), true)

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
    println("All Queries are Done - Avro - PT!")

  }
}
