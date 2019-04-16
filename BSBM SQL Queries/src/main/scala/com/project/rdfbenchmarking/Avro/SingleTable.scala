package com.project.rdfbenchmarking.Avro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class SingleTable {

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

    val RDF_DF = sparkSession.read
      .format("com.databricks.spark.avro")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Avro/SingleTable/SingleTable")
      .toDF()

    RDF_DF.createOrReplaceTempView("SingleTable")

    /*
		 * Query 6: Find products having a label that contains a specific string.
		 *
		 * The consumer remembers parts of a product name from former searches.
		 * He wants to find the product again by searching for the parts of the name that he remembers.
		 * */

    sparkSession.time(
      sparkSession.sql(
        """
          |SELECT T1.subject,
          |       T2.object as label
          |FROM   SingleTable T1,
          |       SingleTable T2
          |WHERE  T1.subject = t2.subject
          |       AND T2.predicate = 'http://www.w3.org/2000/01/rdf-schema#label'
          |       AND T2.object LIKE '%manner%'
          |       AND T1.predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
          |       AND T1.object =
          |'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature'
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
          |SELECT t2.subject AS nr,
          |       t2.object  AS product,
          |       t3.object  AS title,
          |       t4.subject AS nr,
          |       t4.object  AS NAME,
          |       t5.object  AS mbox_sha1sum,
          |       t6.object  AS country
          |FROM   SingleTable t1,
          |       SingleTable t2,
          |       SingleTable t3,
          |       SingleTable t4,
          |       SingleTable t5,
          |       SingleTable t6
          |WHERE  t1.subject =
          |'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review1'
          |AND t1.subject = t2.subject
          |AND t1.subject = t3.subject
          |AND t1.object = t4.subject
          |AND t4.subject = t5.subject
          |AND t4.subject = t6.subject
          |AND t2.predicate =
          |    'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor'
          |AND t3.predicate = 'http://purl.org/dc/elements/1.1/title'
          |AND t1.predicate = 'http://purl.org/stuff/rev#reviewer'
          |AND t4.predicate = 'http://xmlns.com/foaf/0.1/name'
          |AND t5.predicate = 'http://xmlns.com/foaf/0.1/mbox_sha1sum'
          |AND t6.predicate =
          |    'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/country'
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
          |SELECT DISTINCT t1.subject,
          |                t4.object AS price
          |FROM            SingleTable t1,
          |                SingleTable t2,
          |                SingleTable t3,
          |                SingleTable t4,
          |                SingleTable t5,
          |                SingleTable t6
          |WHERE           t1.subject = t2.subject
          |AND             t1.subject = t3.subject
          |AND             t1.subject = t4.subject
          |AND             t1.subject = t5.subject
          |AND             t1.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/deliveryDays'
          |AND             Substring(t1.object,1,Charindex('^',t1.object)-1) <= 3
          |AND             t2.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/validTo'
          |AND             Substring(t2.object,1,Charindex('^',t2.object)-1) > 2008-05-22T00:00:00+
          |AND             t3.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/product'
          |AND             t3.object = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1'
          |AND             t4.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/price'
          |AND             t5.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Vendor1'
          |AND             t5.object = t6.subject
          |AND             t6.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/country'
          |AND             t6.object = 'http://downlode.org/rdf/iso-3166/countries#US'
          |ORDER BY        t4.object limit 10
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
          |SELECT t1.object
          |FROM   SingleTable t1
          |WHERE  t1.subject =
          |'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1'
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
          |SELECT t1.subject   AS productnr,
          |       t2.object    AS productlabel,
          |       t4.object    AS offerurl,
          |       t5.object    AS price,
          |       t6.object    AS deliverydays,
          |       t7.object    AS validto,
          |       t9.object    AS vendorname,
          |       t10.object   AS vendorhomepage
          |FROM   SingleTable t1,
          |       SingleTable t2,
          |       SingleTable t3,
          |       SingleTable t4,
          |       SingleTable t5,
          |       SingleTable t6,
          |       SingleTable t7,
          |       SingleTable t8,
          |       SingleTable t9,
          |       SingleTable t10
          |WHERE  t1.subject = t2.subject
          |AND    t2.predicate = 'http://www.w3.org/2000/01/rdf-schema#label'
          |AND    t3.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/product'
          |AND    t3.object = t1.subject
          |AND    t3.subject = t4.subject
          |AND    t3.subject = t5.subject
          |AND    t3.subject = t6.subject
          |AND    t3.subject = t7.subject
          |AND    t4.subject = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1'
          |AND    t4.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/offerWebpage'
          |AND    t5.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/price'
          |AND    t6.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/deliveryDays'
          |AND    t7 predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/validTo'
          |AND    t8.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/vendor'
          |AND    t8.object = t9.subject
          |AND    t9.subject = t10.subject
          |AND    t9.predicate = 'http://www.w3.org/2000/01/rdf-schema#label'
          |AND    t10.predicate = 'http://xmlns.com/foaf/0.1/homepage'
        """.stripMargin).show()
    )

  }

}
