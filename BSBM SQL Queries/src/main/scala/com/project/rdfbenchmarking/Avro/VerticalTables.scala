package com.project.rdfbenchmarking.Avro

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

    val RDF_DF_COMMENT = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/comment").toDF()
    RDF_DF_COMMENT.createOrReplaceTempView("Comment")

    val RDF_DF_COUNTRY = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/country").toDF()
    RDF_DF_COUNTRY.createOrReplaceTempView("Country")

    val RDF_DF_DATE = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/date").toDF()
    RDF_DF_DATE.createOrReplaceTempView("Date")

    val RDF_DF_DELIVERY_DAYS = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/deliveryDays").toDF()
    RDF_DF_DELIVERY_DAYS.createOrReplaceTempView("DeliveryDays")

    val RDF_DF_HOMEPAGE = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/homepage").toDF()
    RDF_DF_HOMEPAGE.createOrReplaceTempView("Homepage")

    val RDF_DF_LABEL = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/label").toDF()
    RDF_DF_LABEL.createOrReplaceTempView("Label")

    val RDF_DF_MBOX_SHA1SUM = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/mbox_sha1sum").toDF()
    RDF_DF_MBOX_SHA1SUM.createOrReplaceTempView("Mbox_sha1sum")

    val RDF_DF_NAME = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/name").toDF()
    RDF_DF_NAME.createOrReplaceTempView("Name")

    val RDF_DF_OFFERWEBPAGE = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/offerWebpage").toDF()
    RDF_DF_OFFERWEBPAGE.createOrReplaceTempView("OfferWebPage")

    val RDF_DF_RPICE = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/price").toDF()
    RDF_DF_RPICE.createOrReplaceTempView("Price")

    val RDF_DF_PRODUCER = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/Producer").toDF()
    RDF_DF_PRODUCER.createOrReplaceTempView("Producer")

    val RDF_DF_PRODUCT = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/Product").toDF()
    RDF_DF_PRODUCT.createOrReplaceTempView("Product")

    val RDF_DF_PRODUCTFEATURE = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/ProductFeature").toDF()
    RDF_DF_PRODUCTFEATURE.createOrReplaceTempView("ProductFeature")

    val RDF_DF_PRODUCTPROPERTYNUMERIC1 = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/productPropertyNumeric1").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC1.createOrReplaceTempView("ProductPropertyNumeric1")

    val RDF_DF_PRODUCTPROPERTYNUMERIC2 = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/productPropertyNumeric2").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC2.createOrReplaceTempView("ProductPropertyNumeric2")

    val RDF_DF_PRODUCTPROPERTYNUMERIC3 = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/productPropertyNumeric3").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC3.createOrReplaceTempView("ProductPropertyNumeric3")

    val RDF_DF_PRODUCTPROPERTYNUMERIC5 = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/productPropertyNumeric5").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC5.createOrReplaceTempView("ProductPropertyNumeric5")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL1 = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/productPropertyTextual1").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL1.createOrReplaceTempView("ProductPropertyTextual1")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL2 = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/productPropertyTextual2").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL2.createOrReplaceTempView("ProductPropertyTextual2")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL3 = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/productPropertyTextual3").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL3.createOrReplaceTempView("ProductPropertyTextual3")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL4 = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/productPropertyTextual4").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL4.createOrReplaceTempView("ProductPropertyTextual4")

    val RDF_DF_PUBLISHER = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/publisher").toDF()
    RDF_DF_PUBLISHER.createOrReplaceTempView("Publisher")

    val RDF_DF_RATING1 = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/rating1").toDF()
    RDF_DF_RATING1.createOrReplaceTempView("Rating1")

    val RDF_DF_RATING2 = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/rating2").toDF()
    RDF_DF_RATING2.createOrReplaceTempView("Rating2")

    val RDF_DF_RATING3 = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/rating3").toDF()
    RDF_DF_RATING3.createOrReplaceTempView("Rating3")

    val RDF_DF_RATING4 = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/rating4").toDF()
    RDF_DF_RATING4.createOrReplaceTempView("Rating4")

    val RDF_DF_REVIEWDATE = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/reviewDate").toDF()
    RDF_DF_REVIEWDATE.createOrReplaceTempView("ReviewDate")

    val RDF_DF_REVIEWER = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/Reviewer").toDF()
    RDF_DF_REVIEWER.createOrReplaceTempView("Reviewer")

    val RDF_DF_REVIEWFOR = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/reviewFor").toDF()
    RDF_DF_REVIEWFOR.createOrReplaceTempView("ReviewFor")

    val RDF_DF_SUBCLASSOF = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/subClassOf").toDF()
    RDF_DF_SUBCLASSOF.createOrReplaceTempView("SubclassOf")

    val RDF_DF_TEXT = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/text").toDF()
    RDF_DF_TEXT.createOrReplaceTempView("Text")

    val RDF_DF_TITLE = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/title").toDF()
    RDF_DF_TITLE.createOrReplaceTempView("Title")

    val RDF_DF_TYPE = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/type").toDF()
    RDF_DF_TYPE.createOrReplaceTempView("Type")

    val RDF_DF_VALIDFROM = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/validFrom").toDF()
    RDF_DF_VALIDFROM.createOrReplaceTempView("ValidFrom")

    val RDF_DF_VALIDTO = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/validTo").toDF()
    RDF_DF_VALIDTO.createOrReplaceTempView("ValidTo")

    val RDF_DF_VENDOR = sparkSession.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/VerticalTables/Vendor").toDF()
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
