package com.project.rdfbenchmarking.Parquet

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class VerticalTables {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("SQLSPARK")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.setLogLevel("ERROR")

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()

    val RDF_DF_COMMENT = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/comment").toDF()
    RDF_DF_COMMENT.createOrReplaceTempView("Comment")

    val RDF_DF_COUNTRY = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/country").toDF()
    RDF_DF_COUNTRY.createOrReplaceTempView("Country")

    val RDF_DF_DATE = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/date").toDF()
    RDF_DF_DATE.createOrReplaceTempView("Date")

    val RDF_DF_DELIVERY_DAYS = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/deliveryDays").toDF()
    RDF_DF_DELIVERY_DAYS.createOrReplaceTempView("DeliveryDays")

    val RDF_DF_HOMEPAGE = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/homepage").toDF()
    RDF_DF_HOMEPAGE.createOrReplaceTempView("Homepage")

    val RDF_DF_LABEL = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/label").toDF()
    RDF_DF_LABEL.createOrReplaceTempView("Label")

    val RDF_DF_MBOX_SHA1SUM = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/mbox_sha1sum").toDF()
    RDF_DF_MBOX_SHA1SUM.createOrReplaceTempView("Mbox_sha1sum")

    val RDF_DF_NAME = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/name").toDF()
    RDF_DF_NAME.createOrReplaceTempView("Name")

    val RDF_DF_OFFERWEBPAGE = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/offerWebpage").toDF()
    RDF_DF_OFFERWEBPAGE.createOrReplaceTempView("OfferWebPage")

    val RDF_DF_RPICE = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/price").toDF()
    RDF_DF_RPICE.createOrReplaceTempView("Price")

    val RDF_DF_PRODUCER = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/Producer").toDF()
    RDF_DF_PRODUCER.createOrReplaceTempView("Producer")

    val RDF_DF_PRODUCT = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/Product").toDF()
    RDF_DF_PRODUCT.createOrReplaceTempView("Product")

    val RDF_DF_PRODUCTFEATURE = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/ProductFeature").toDF()
    RDF_DF_PRODUCTFEATURE.createOrReplaceTempView("ProductFeature")

    val RDF_DF_PRODUCTPROPERTYNUMERIC1 = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/productPropertyNumeric1").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC1.createOrReplaceTempView("ProductPropertyNumeric1")

    val RDF_DF_PRODUCTPROPERTYNUMERIC2 = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/productPropertyNumeric2").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC2.createOrReplaceTempView("ProductPropertyNumeric2")

    val RDF_DF_PRODUCTPROPERTYNUMERIC3 = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/productPropertyNumeric3").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC3.createOrReplaceTempView("ProductPropertyNumeric3")

    val RDF_DF_PRODUCTPROPERTYNUMERIC5 = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/productPropertyNumeric5").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC5.createOrReplaceTempView("ProductPropertyNumeric5")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL1 = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/productPropertyTextual1").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL1.createOrReplaceTempView("ProductPropertyTextual1")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL2 = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/productPropertyTextual2").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL2.createOrReplaceTempView("ProductPropertyTextual2")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL3 = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/productPropertyTextual3").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL3.createOrReplaceTempView("ProductPropertyTextual3")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL4 = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/productPropertyTextual4").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL4.createOrReplaceTempView("ProductPropertyTextual4")

    val RDF_DF_PUBLISHER = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/publisher").toDF()
    RDF_DF_PUBLISHER.createOrReplaceTempView("Publisher")

    val RDF_DF_RATING1 = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/rating1").toDF()
    RDF_DF_RATING1.createOrReplaceTempView("Rating1")

    val RDF_DF_RATING2 = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/rating2").toDF()
    RDF_DF_RATING2.createOrReplaceTempView("Rating2")

    val RDF_DF_RATING3 = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/rating3").toDF()
    RDF_DF_RATING3.createOrReplaceTempView("Rating3")

    val RDF_DF_RATING4 = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/rating4").toDF()
    RDF_DF_RATING4.createOrReplaceTempView("Rating4")

    val RDF_DF_REVIEWDATE = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/reviewDate").toDF()
    RDF_DF_REVIEWDATE.createOrReplaceTempView("ReviewDate")

    val RDF_DF_REVIEWER = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/Reviewer").toDF()
    RDF_DF_REVIEWER.createOrReplaceTempView("Reviewer")

    val RDF_DF_REVIEWFOR = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/reviewFor").toDF()
    RDF_DF_REVIEWFOR.createOrReplaceTempView("ReviewFor")

    val RDF_DF_SUBCLASSOF = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/subClassOf").toDF()
    RDF_DF_SUBCLASSOF.createOrReplaceTempView("SubclassOf")

    val RDF_DF_TEXT = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/text").toDF()
    RDF_DF_TEXT.createOrReplaceTempView("Text")

    val RDF_DF_TITLE = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/title").toDF()
    RDF_DF_TITLE.createOrReplaceTempView("Title")

    val RDF_DF_TYPE = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/type").toDF()
    RDF_DF_TYPE.createOrReplaceTempView("Type")

    val RDF_DF_VALIDFROM = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/validFrom").toDF()
    RDF_DF_VALIDFROM.createOrReplaceTempView("ValidFrom")

    val RDF_DF_VALIDTO = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/validTo").toDF()
    RDF_DF_VALIDTO.createOrReplaceTempView("ValidTo")

    val RDF_DF_VENDOR = sparkSession.read.format("parquet").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/VerticalTables/Vendor").toDF()
    RDF_DF_VENDOR.createOrReplaceTempView("Vendor")

    /*
		 * Query 6: Find products having a label that contains a specific string.
		 *
		 * The consumer remembers parts of a product name from former searches.
		 * He wants to find the product again by searching for the parts of the name that he remembers.
		 * */

    sparkSession.time(
      sparkSession.sql(
        """
          |
        """.stripMargin).show()
    )

    /*
		 * Query 9: Get information about a reviewer.
		 *
		 * In order to decide whether to trust a review, the consumer asks for any
		 * kind of information that is available about the reviewer.
		 * */

    sparkSession.time(
      sparkSession.sql(
        """
          |
        """.stripMargin).show()
    )

    /*
		 * Query 10: Get offers for a given product which fulfill specific requirements.
		 *
		 * The consumer wants to buy from a vendor in the United States that is able to
		 * deliver within 3 days and is looking for the cheapest offer that fulfills these requirements.
		 * */

    sparkSession.time(
      sparkSession.sql(
        """
          |
        """.stripMargin).show()
    )

    /*
		 * Query 11: Get all information about an offer.
		 *
		 * After deciding on a specific offer, the consumer wants to get all information
		 * that is directly related to this offer.
		 * */

    sparkSession.time(
      sparkSession.sql(
        """
          |
        """.stripMargin).show()
    )

    /*
		 * Query 12: Export information about an offer into another schemata.
		 *
		 * After deciding on a specific offer, the consumer wants to save information
		 * about this offer on his local machine using a different RDF schema.
		 * */

    sparkSession.time(
      sparkSession.sql(
        """
          |
        """.stripMargin).show()
    )

  }

}
