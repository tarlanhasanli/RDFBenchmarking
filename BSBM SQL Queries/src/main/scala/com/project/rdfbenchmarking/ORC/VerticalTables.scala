package com.project.rdfbenchmarking.ORC

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
      .master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()

    val RDF_DF_COMMENT = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/comment").toDF()
    RDF_DF_COMMENT.createOrReplaceTempView("Comment")

    val RDF_DF_COUNTRY = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/country").toDF()
    RDF_DF_COUNTRY.createOrReplaceTempView("Country")

    val RDF_DF_DATE = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/date").toDF()
    RDF_DF_DATE.createOrReplaceTempView("Date")

    val RDF_DF_DELIVERY_DAYS = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/deliveryDays").toDF()
    RDF_DF_DELIVERY_DAYS.createOrReplaceTempView("DeliveryDays")

    val RDF_DF_HOMEPAGE = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/homepage").toDF()
    RDF_DF_HOMEPAGE.createOrReplaceTempView("Homepage")

    val RDF_DF_LABEL = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/label").toDF()
    RDF_DF_LABEL.createOrReplaceTempView("Label")

    val RDF_DF_MBOX_SHA1SUM = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/mbox_sha1sum").toDF()
    RDF_DF_MBOX_SHA1SUM.createOrReplaceTempView("Mbox_sha1sum")

    val RDF_DF_NAME = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/name").toDF()
    RDF_DF_NAME.createOrReplaceTempView("Name")

    val RDF_DF_OFFERWEBPAGE = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/offerWebpage").toDF()
    RDF_DF_OFFERWEBPAGE.createOrReplaceTempView("OfferWebPage")

    val RDF_DF_RPICE = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/price").toDF()
    RDF_DF_RPICE.createOrReplaceTempView("Price")

    val RDF_DF_PRODUCER = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/Producer").toDF()
    RDF_DF_PRODUCER.createOrReplaceTempView("Producer")

    val RDF_DF_PRODUCT = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/Product").toDF()
    RDF_DF_PRODUCT.createOrReplaceTempView("Product")

    val RDF_DF_PRODUCTFEATURE = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/ProductFeature").toDF()
    RDF_DF_PRODUCTFEATURE.createOrReplaceTempView("ProductFeature")

    val RDF_DF_PRODUCTPROPERTYNUMERIC1 = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/productPropertyNumeric1").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC1.createOrReplaceTempView("ProductPropertyNumeric1")

    val RDF_DF_PRODUCTPROPERTYNUMERIC2 = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/productPropertyNumeric2").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC2.createOrReplaceTempView("ProductPropertyNumeric2")

    val RDF_DF_PRODUCTPROPERTYNUMERIC3 = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/productPropertyNumeric3").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC3.createOrReplaceTempView("ProductPropertyNumeric3")

    val RDF_DF_PRODUCTPROPERTYNUMERIC5 = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/productPropertyNumeric5").toDF()
    RDF_DF_PRODUCTPROPERTYNUMERIC5.createOrReplaceTempView("ProductPropertyNumeric5")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL1 = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/productPropertyTextual1").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL1.createOrReplaceTempView("ProductPropertyTextual1")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL2 = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/productPropertyTextual2").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL2.createOrReplaceTempView("ProductPropertyTextual2")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL3 = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/productPropertyTextual3").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL3.createOrReplaceTempView("ProductPropertyTextual3")

    val RDF_DF_PRODUCTPROPERTYTEXTUAL4 = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/productPropertyTextual4").toDF()
    RDF_DF_PRODUCTPROPERTYTEXTUAL4.createOrReplaceTempView("ProductPropertyTextual4")

    val RDF_DF_PUBLISHER = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/publisher").toDF()
    RDF_DF_PUBLISHER.createOrReplaceTempView("Publisher")

    val RDF_DF_RATING1 = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/rating1").toDF()
    RDF_DF_RATING1.createOrReplaceTempView("Rating1")

    val RDF_DF_RATING2 = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/rating2").toDF()
    RDF_DF_RATING2.createOrReplaceTempView("Rating2")

    val RDF_DF_RATING3 = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/rating3").toDF()
    RDF_DF_RATING3.createOrReplaceTempView("Rating3")

    val RDF_DF_RATING4 = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/rating4").toDF()
    RDF_DF_RATING4.createOrReplaceTempView("Rating4")

    val RDF_DF_REVIEWDATE = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/reviewDate").toDF()
    RDF_DF_REVIEWDATE.createOrReplaceTempView("ReviewDate")

    val RDF_DF_REVIEWER = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/Reviewer").toDF()
    RDF_DF_REVIEWER.createOrReplaceTempView("Reviewer")

    val RDF_DF_REVIEWFOR = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/reviewFor").toDF()
    RDF_DF_REVIEWFOR.createOrReplaceTempView("ReviewFor")

    val RDF_DF_SUBCLASSOF = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/subClassOf").toDF()
    RDF_DF_SUBCLASSOF.createOrReplaceTempView("SubclassOf")

    val RDF_DF_TEXT = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/text").toDF()
    RDF_DF_TEXT.createOrReplaceTempView("Text")

    val RDF_DF_TITLE = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/title").toDF()
    RDF_DF_TITLE.createOrReplaceTempView("Title")

    val RDF_DF_TYPE = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/type").toDF()
    RDF_DF_TYPE.createOrReplaceTempView("Type")

    val RDF_DF_VALIDFROM = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/validFrom").toDF()
    RDF_DF_VALIDFROM.createOrReplaceTempView("ValidFrom")

    val RDF_DF_VALIDTO = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/validTo").toDF()
    RDF_DF_VALIDTO.createOrReplaceTempView("ValidTo")

    val RDF_DF_VENDOR = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("/user/cloudera/RDFBenchHDFS/ORC/VerticalTables/Vendor").toDF()
    RDF_DF_VENDOR.createOrReplaceTempView("Vendor")

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
