package com.project.rdfbenchmarking.Hive

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

    val warehouseLocation = "/user/hive/warehouse"

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris","thrift://quickstart.cloudera:9083")
      .enableHiveSupport()
      .getOrCreate()

    val RDF_DF_COMMENT = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/comment.csv").toDF()
    RDF_DF_COMMENT.createOrReplaceTempView("CommentDF")
    sparkSession.sql("create table if not exists Comment as select * from CommentDF")

    val RDF_DF_COUNTRY = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/country.csv").toDF()
    RDF_DF_COUNTRY.createOrReplaceTempView("CountryDF")
    sparkSession.sql("create table if not exists Country as select * from CountryDF")

    val RDF_DF_DATE = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/date.csv").toDF()
    RDF_DF_DATE.createOrReplaceTempView("DateDF")
    sparkSession.sql("create table if not exists Date as select * from DateDF")

    val RDF_DF_DELIVERY_DAYS = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/deliveryDays.csv").toDF()
    RDF_DF_DELIVERY_DAYS.createOrReplaceTempView("DeliveryDaysDF")
    sparkSession.sql("create table if not exists DeliveryDays as select * from DeliveryDaysDF")

    val RDF_DF_HOMEPAGE = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/homepage.csv").toDF()
    RDF_DF_HOMEPAGE.createOrReplaceTempView("HomepageDF")
    sparkSession.sql("create table if not exists Homepage as select * from HomepageDF")

    val RDF_DF_LABEL = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/label.csv").toDF()
    RDF_DF_LABEL.createOrReplaceTempView("LabelDF")
    sparkSession.sql("create table if not exists Label as select * from LabelDF")

    val RDF_DF_MBOX_SHA1SUM = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/mbox_sha1sum.csv").toDF()
    RDF_DF_MBOX_SHA1SUM.createOrReplaceTempView("Mbox_sha1sumDF")
    sparkSession.sql("create table if not exists Mbox_sha1sum as select * from Mbox_sha1sumDF")

    val RDF_DF_NAME = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/name.csv").toDF()
    RDF_DF_NAME.createOrReplaceTempView("NameDF")
    sparkSession.sql("create table if not exists Name as select * from NameDF")

    val RDF_DF_OFFERWEBPAGE = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/offerWebpage.csv").toDF()
    RDF_DF_OFFERWEBPAGE.createOrReplaceTempView("OfferWebPageDF")
    sparkSession.sql("create table if not exists OfferWebPage as select * from OfferWebPageDF")

    val RDF_DF_RPICE = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/price.csv").toDF()
    RDF_DF_RPICE.createOrReplaceTempView("PriceDF")
    sparkSession.sql("create table if not exists Price as select * from PriceDF")

    val RDF_DF_PRODUCER = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/Producer.csv").toDF()
    RDF_DF_PRODUCER.createOrReplaceTempView("ProducerDF")
    sparkSession.sql("create table if not exists Producer as select * from ProducerDF")

    val RDF_DF_PRODUCT = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/Product.csv").toDF()
    RDF_DF_PRODUCT.createOrReplaceTempView("ProductDF")
    sparkSession.sql("create table if not exists Product as select * from ProductDF")

    val RDF_DF_PRODUCTFEATURE = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/ProductFeature.csv").toDF()
    RDF_DF_PRODUCTFEATURE.createOrReplaceTempView("ProductFeatureDF")
    sparkSession.sql("create table if not exists ProductFeature as select * from ProductFeatureDF")

    val RDF_DF_PRODUCTPROPERTYNUMERIC1 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/productPropertyNumeric1.csv").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC1.createOrReplaceTempView("ProductPropertyNumeric1DF")
    sparkSession.sql("create table if not exists ProductPropertyNumeric1 as select * from ProductPropertyNumeric1DF")

    val RDF_DF_PRODUCTPROPERTYNUMERIC2 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/productPropertyNumeric2.csv").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC2.createOrReplaceTempView("ProductPropertyNumeric2DF")
    sparkSession.sql("create table if not exists ProductPropertyNumeric2 as select * from ProductPropertyNumeric2DF")

    val RDF_DF_PRODUCTPROPERTYNUMERIC3 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/productPropertyNumeric3.csv").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC3.createOrReplaceTempView("ProductPropertyNumeric3DF")
    sparkSession.sql("create table if not exists ProductPropertyNumeric3 as select * from ProductPropertyNumeric3DF")

    val RDF_DF_PRODUCTPROPERTYNUMERIC5 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/productPropertyNumeric5.csv").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC5.createOrReplaceTempView("ProductPropertyNumeric5DF")
    sparkSession.sql("create table if not exists ProductPropertyNumeric5 as select * from ProductPropertyNumeric5DF")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL1 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/productPropertyTextual1.csv").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL1.createOrReplaceTempView("ProductPropertyTextual1DF")
    sparkSession.sql("create table if not exists ProductPropertyTextual1 as select * from ProductPropertyTextual1DF")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL2 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/productPropertyTextual2.csv").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL2.createOrReplaceTempView("ProductPropertyTextual2DF")
    sparkSession.sql("create table if not exists ProductPropertyTextual2 as select * from ProductPropertyTextual2DF")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL3 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/productPropertyTextual3.csv").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL3.createOrReplaceTempView("ProductPropertyTextual3DF")
    sparkSession.sql("create table if not exists ProductPropertyTextual3 as select * from ProductPropertyTextual3DF")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL4 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/productPropertyTextual4.csv").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL4.createOrReplaceTempView("ProductPropertyTextual4DF")
    sparkSession.sql("create table if not exists ProductPropertyTextual4 as select * from ProductPropertyTextual4DF")

    val RDF_DF_PUBLISHER = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/publisher.csv").toDF()
    RDF_DF_PUBLISHER.createOrReplaceTempView("PublisherDF")
    sparkSession.sql("create table if not exists Publisher as select * from PublisherDF")

    val RDF_DF_RATING1 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/rating1.csv").toDF()
    RDF_DF_RATING1.createOrReplaceTempView("Rating1DF")
    sparkSession.sql("create table if not exists Rating1 as select * from Rating1DF")

    val RDF_DF_RATING2 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/rating2.csv").toDF()
    RDF_DF_RATING2.createOrReplaceTempView("Rating2DF")
    sparkSession.sql("create table if not exists Rating2 as select * from Rating2DF")

    val RDF_DF_RATING3 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/rating3.csv").toDF()
    RDF_DF_RATING3.createOrReplaceTempView("Rating3DF")
    sparkSession.sql("create table if not exists Rating3 as select * from Rating3DF")

    val RDF_DF_RATING4 = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/rating4.csv").toDF()
    RDF_DF_RATING4.createOrReplaceTempView("Rating4DF")
    sparkSession.sql("create table if not exists Rating4 as select * from Rating4DF")

    val RDF_DF_REVIEWDATE = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/reviewDate.csv").toDF()
    RDF_DF_REVIEWDATE.createOrReplaceTempView("ReviewDateDF")
    sparkSession.sql("create table if not exists ReviewDate as select * from ReviewDateDF")

    val RDF_DF_REVIEWER = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/Reviewer.csv").toDF()
    RDF_DF_REVIEWER.createOrReplaceTempView("ReviewerDF")
    sparkSession.sql("create table if not exists Reviewer as select * from ReviewerDF")

    val RDF_DF_REVIEWFOR = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/reviewFor.csv").toDF()
    RDF_DF_REVIEWFOR.createOrReplaceTempView("ReviewForDF")
    sparkSession.sql("create table if not exists ReviewFor as select * from ReviewForDF")

    val RDF_DF_SUBCLASSOF = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/subClassOf.csv").toDF()
    RDF_DF_SUBCLASSOF.createOrReplaceTempView("SubclassOfDF")
    sparkSession.sql("create table if not exists SubclassOf as select * from SubclassOfDF")

    val RDF_DF_TEXT = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/text.csv").toDF()
    RDF_DF_TEXT.createOrReplaceTempView("TextDF")
    sparkSession.sql("create table if not exists Text as select * from TextDF")

    val RDF_DF_TITLE = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/title.csv").toDF()
    RDF_DF_TITLE.createOrReplaceTempView("TitleDF")
    sparkSession.sql("create table if not exists Title as select * from TitleDF")

    val RDF_DF_TYPE = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/type.csv").toDF()
    RDF_DF_TYPE.createOrReplaceTempView("TypeDF")
    sparkSession.sql("create table if not exists Type as select * from TypeDF")

    val RDF_DF_VALIDFROM = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/validFrom.csv").toDF()
    RDF_DF_VALIDFROM.createOrReplaceTempView("ValidFromDF")
    sparkSession.sql("create table if not exists ValidFrom as select * from ValidFromDF")

    val RDF_DF_VALIDTO = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/validTo.csv").toDF()
    RDF_DF_VALIDTO.createOrReplaceTempView("ValidToDF")
    sparkSession.sql("create table if not exists ValidTo as select * from ValidToDF")

    val RDF_DF_VENDOR = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/Vendor.csv").toDF()
    RDF_DF_VENDOR.createOrReplaceTempView("VendorDF")
    sparkSession.sql("create table if not exists Vendor as select * from VendorDF")

    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(sparkSession)

    stageMetrics.runAndMeasure(sparkSession.sql(new VerticalTableQueries query1).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new VerticalTableQueries query2).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new VerticalTableQueries query3).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new VerticalTableQueries query4).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new VerticalTableQueries query5).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new VerticalTableQueries query6).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new VerticalTableQueries query7).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new VerticalTableQueries query8).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new VerticalTableQueries query9).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new VerticalTableQueries query10).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new VerticalTableQueries query11).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new VerticalTableQueries query12).show())

  }

}
