package com.project.rdfbenchmarking.CSV

import com.project.rdfbenchmarking.Queries.VerticalTableQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object VerticalTables {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("SQLSPARK")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.setLogLevel("ERROR")

    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val RDF_DF_COMMENT = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/comment.csv").toDF()
    RDF_DF_COMMENT.createOrReplaceTempView("Comment")

    val RDF_DF_COUNTRY = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/country.csv").toDF()
    RDF_DF_COUNTRY.createOrReplaceTempView("Country")

    val RDF_DF_DATE = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/date.csv").toDF()
    RDF_DF_DATE.createOrReplaceTempView("Date")

    val RDF_DF_DELIVERY_DAYS = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/deliveryDays.csv").toDF()
    RDF_DF_DELIVERY_DAYS.createOrReplaceTempView("DeliveryDays")

    val RDF_DF_HOMEPAGE = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/homepage.csv").toDF()
    RDF_DF_HOMEPAGE.createOrReplaceTempView("Homepage")

    val RDF_DF_LABEL = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/label.csv").toDF()
    RDF_DF_LABEL.createOrReplaceTempView("Label")

    val RDF_DF_MBOX_SHA1SUM = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/mbox_sha1sum.csv").toDF()
    RDF_DF_MBOX_SHA1SUM.createOrReplaceTempView("Mbox_sha1sum")

    val RDF_DF_NAME = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/name.csv").toDF()
    RDF_DF_NAME.createOrReplaceTempView("Name")

    val RDF_DF_OFFERWEBPAGE = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/offerWebpage.csv").toDF()
    RDF_DF_OFFERWEBPAGE.createOrReplaceTempView("OfferWebPage")

    val RDF_DF_RPICE = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/price.csv").toDF()
    RDF_DF_RPICE.createOrReplaceTempView("Price")

    val RDF_DF_PRODUCER = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/Producer.csv").toDF()
    RDF_DF_PRODUCER.createOrReplaceTempView("Producer")

    val RDF_DF_PRODUCT = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/Product.csv").toDF()
    RDF_DF_PRODUCT.createOrReplaceTempView("Product")

    val RDF_DF_PRODUCTFEATURE = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/ProductFeature.csv").toDF()
    RDF_DF_PRODUCTFEATURE.createOrReplaceTempView("ProductFeature")

    val RDF_DF_PRODUCTPROPERTYNUMERIC1 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/productPropertyNumeric1.csv").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC1.createOrReplaceTempView("ProductPropertyNumeric1")

    val RDF_DF_PRODUCTPROPERTYNUMERIC2 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/productPropertyNumeric2.csv").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC2.createOrReplaceTempView("ProductPropertyNumeric2")

    val RDF_DF_PRODUCTPROPERTYNUMERIC3 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/productPropertyNumeric3.csv").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC3.createOrReplaceTempView("ProductPropertyNumeric3")

    val RDF_DF_PRODUCTPROPERTYNUMERIC5 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/productPropertyNumeric5.csv").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC5.createOrReplaceTempView("ProductPropertyNumeric5")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL1 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/productPropertyTextual1.csv").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL1.createOrReplaceTempView("ProductPropertyTextual1")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL2 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/productPropertyTextual2.csv").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL2.createOrReplaceTempView("ProductPropertyTextual2")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL3 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/productPropertyTextual3.csv").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL3.createOrReplaceTempView("ProductPropertyTextual3")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL4 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/productPropertyTextual4.csv").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL4.createOrReplaceTempView("ProductPropertyTextual4")

    val RDF_DF_PUBLISHER = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/publisher.csv").toDF()
    RDF_DF_PUBLISHER.createOrReplaceTempView("Publisher")

    val RDF_DF_RATING1 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/rating1.csv").toDF()
    RDF_DF_RATING1.createOrReplaceTempView("Rating1")

    val RDF_DF_RATING2 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/rating2.csv").toDF()
    RDF_DF_RATING2.createOrReplaceTempView("Rating2")

    val RDF_DF_RATING3 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/rating3.csv").toDF()
    RDF_DF_RATING3.createOrReplaceTempView("Rating3")

    val RDF_DF_RATING4 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/rating4.csv").toDF()
    RDF_DF_RATING4.createOrReplaceTempView("Rating4")

    val RDF_DF_REVIEWDATE = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/reviewDate.csv").toDF()
    RDF_DF_REVIEWDATE.createOrReplaceTempView("ReviewDate")

    val RDF_DF_REVIEWER = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/Reviewer.csv").toDF()
    RDF_DF_REVIEWER.createOrReplaceTempView("Reviewer")

    val RDF_DF_REVIEWFOR = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/reviewFor.csv").toDF()
    RDF_DF_REVIEWFOR.createOrReplaceTempView("ReviewFor")

    val RDF_DF_SUBCLASSOF = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/subClassOf.csv").toDF()
    RDF_DF_SUBCLASSOF.createOrReplaceTempView("SubclassOf")

    val RDF_DF_TEXT = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/text.csv").toDF()
    RDF_DF_TEXT.createOrReplaceTempView("Text")

    val RDF_DF_TITLE = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/title.csv").toDF()
    RDF_DF_TITLE.createOrReplaceTempView("Title")

    val RDF_DF_TYPE = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/type.csv").toDF()
    RDF_DF_TYPE.createOrReplaceTempView("Type")

    val RDF_DF_VALIDFROM = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/validFrom.csv").toDF()
    RDF_DF_VALIDFROM.createOrReplaceTempView("ValidFrom")

    val RDF_DF_VALIDTO = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/validTo.csv").toDF()
    RDF_DF_VALIDTO.createOrReplaceTempView("ValidTo")

    val RDF_DF_VENDOR = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/Vendor.csv").toDF()
    RDF_DF_VENDOR.createOrReplaceTempView("Vendor")

    sparkSession.time(
      sparkSession.sql(new VerticalTableQueries query6).show()
    )

    sparkSession.time(
      sparkSession.sql(new VerticalTableQueries query9).show()
    )

    sparkSession.time(
      sparkSession.sql(new VerticalTableQueries query10).show()
    )

    sparkSession.time(
      sparkSession.sql(new VerticalTableQueries query11).show()
    )

    sparkSession.time(
      sparkSession.sql(new VerticalTableQueries query12).show()
    )

  }

}
