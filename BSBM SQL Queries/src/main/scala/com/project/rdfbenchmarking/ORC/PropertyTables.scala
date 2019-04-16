package com.project.rdfbenchmarking.ORC

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class PropertyTables {

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

    val RDF_DF_OFFER = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/ORC/PropertyTables/Offer").toDF()
    RDF_DF_OFFER.createOrReplaceTempView("Offer")

    val RDF_DF_Person = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/ORC/PropertyTables/Person").toDF()
    RDF_DF_Person.createOrReplaceTempView("Person")

    val RDF_DF_Producer = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/ORC/PropertyTables/producer").toDF()
    RDF_DF_Producer.createOrReplaceTempView("Producer")

    val RDF_DF_Product = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/ORC/PropertyTables/product").toDF()
    RDF_DF_Product.createOrReplaceTempView("Product")

    val RDF_DF_ProductFeature = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/ORC/PropertyTables/productFeature").toDF()
    RDF_DF_ProductFeature.createOrReplaceTempView("ProductFeature")

    val RDF_DF_ProductType = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/ORC/PropertyTables/ProductType").toDF()
    RDF_DF_ProductType.createOrReplaceTempView("ProductType")

    val RDF_DF_Review = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/ORC/PropertyTables/Review").toDF()
    RDF_DF_Review.createOrReplaceTempView("Review")

    val RDF_DF_Vendor = sparkSession.read.format("org.apache.spark.sql.execution.datasources.orc").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/ORC/PropertyTables/vendor").toDF()
    RDF_DF_Vendor.createOrReplaceTempView("Vendor")



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
