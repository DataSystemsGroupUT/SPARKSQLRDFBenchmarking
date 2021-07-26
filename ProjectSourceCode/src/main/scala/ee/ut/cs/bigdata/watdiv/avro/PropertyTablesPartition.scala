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
    /*
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

     */

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
      /*
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

       */

      println("AVRO PT partitioned and saved! Horizontal partitioning!")
    }

    else if (partitionType.toLowerCase == "predicate") {
      println("Avro PT partitioned and saved! Predicate based partitioning!")

    }


  }
}
