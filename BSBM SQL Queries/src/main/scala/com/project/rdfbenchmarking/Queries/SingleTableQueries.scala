package com.project.rdfbenchmarking.Queries

class SingleTableQueries {

  /*
  * Query 1: Find products for a given set of generic features.
  *
  * A consumer is looking for a product and has a general idea about what he wants.
  * */

  val query1 =
  """
    |
    """.stripMargin

  /*
  * Query 2: Retrieve basic information about a specific product for display purposes
  *
  * The consumer wants to view basic information about products found by query 1.
  * */

  val query2 =
  """
    |
    """.stripMargin

  /*
  * Query 3: Find products having some specific features and not having one feature
  *
  * After looking at information about some products,
  * the consumer has a more specific idea what we wants.
  * Therefore, he asks for products having several features but not having a specific other feature.
  * */

  val query3 =
  """
    |
    """.stripMargin

  /*
  * Query 4: Find products matching two different sets of features.
  *
  * After looking at information about some products,
  * the consumer has a more specific idea what we wants.
  * Therefore, he asks for products matching either one set of features or another set.
  * */

  val query4 =
  """
    |
    """.stripMargin

  /*
  * Query 5: Find product that are similar to a given product.
  *
  * The consumer has found a product that fulfills his requirements.
  * He now wants to find products with similar features.
  * */

  val query5 =
  """
    |
    """.stripMargin

  /*
   * Query 6: Find products having a label that contains a specific string.
   *
   * The consumer remembers parts of a product name from former searches.
   * He wants to find the product again by searching for the parts of the name that he remembers.
   * */

  /*
   * Query 6: Find products having a label that contains a specific string.
   *
   * The consumer remembers parts of a product name from former searches.
   * He wants to find the product again by searching for the parts of the name that he remembers.
   * */

  val query6 =
    """
     |Select t1.subject, t2.object as label
     |from SingleTable t1, SingleTable t2
     |where t1.subject = t2.subject
     |and t2.object like '%manner%'
     |and t2.predicate = 'http://www.w3.org/2000/01/rdf-schema#label'
     |and trim(t1.object) = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product'
     |and t1.predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
   """.stripMargin

  /*
  * Query 7: Retrieve in-depth information about a specific product including offers and reviews.
  *
  * The consumer has found a products which fulfills his requirements.
  * Now he wants in-depth information about this product including offers from
  * German vendors and product reviews if existent.
  * */

  val query7 =
  """
    |
    """.stripMargin

  /*
  * Query 8: Give me recent reviews in English for a specific product.
  *
  * TThe consumer wants to read the 20 most recent
  * English language reviews about a specific product.
  * */

  val query8 =
  """
    |
    """.stripMargin

  /*
   * Query 9: Get information about a reviewer.
   *
   * In order to decide whether to trust a review, the consumer asks for any
   * kind of information that is available about the reviewer.
   * */

  val query9 =
    """
     |SELECT t2.subject as nr, t2.object as product,
     |t3.object as title, t4.subject as nr,
     |t4.object as name, t5.object as mbox_sha1sum,
     |t6.object as country
     |FROM SingleTable t1, SingleTable t2, SingleTable t3,
     |                    SingleTable t4, SingleTable t5, SingleTable t6, SingleTable t7
     |WHERE t7.subject = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review1'
     |AND t1.subject = t2.subject AND t1.subject = t3.subject AND trim(t1.object) = t4.subject
     |AND trim(t7.object) = t4.subject AND t7.predicate = 'http://purl.org/stuff/rev#reviewer'
     |AND t4.subject = t5.subject AND t4.subject = t6.subject
     |AND t2.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor'
     |AND t3.predicate = 'http://purl.org/dc/elements/1.1/title'
     |AND t1.predicate = 'http://purl.org/stuff/rev#reviewer'
     |AND t4.predicate = 'http://xmlns.com/foaf/0.1/name'
     |AND t5.predicate = 'http://xmlns.com/foaf/0.1/mbox_sha1sum'
     |AND t6.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/country'
   """.stripMargin

  /*
   * Query 10: Get offers for a given product which fulfill specific requirements.
   *
   * The consumer wants to buy from a vendor in the United States that is able to
   * deliver within 3 days and is looking for the cheapest offer that fulfills these requirements.
   * */

  val query10 =
    """
      |Select distinct t1.subject, SUBSTRING(t4.object,1,instr(t4.object, '^')-1) as price
      |from SingleTable t1, SingleTable t2, SingleTable t3,
      |SingleTable t4, SingleTable t5, SingleTable t6
      |where t1.subject = t2.subject and t1.subject = t3.subject
      |and t1.subject = t4.subject and t1.subject = t5.subject
      |and t1.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/deliveryDays'
      |and SUBSTRING(t1.object,1,instr(t1.object, '^')-1) <= 3
      |and t2.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/validTo'
      |and CAST(SUBSTRING(t2.object,1,instr(t2.object,'^')-1) AS DATE) > CAST('2008-05-01T00:00:00' AS DATE)
      |and t3.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/product'
      |and trim(t3.object) = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1'
      |and t4.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/price'
      |and t5.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/vendor'
      |and trim(t5.object) = t6.subject
      |and t6.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/country'
      |and trim(t6.object) = 'http://downlode.org/rdf/iso-3166/countries#GB'
      |order by price
      |limit 10
    """.stripMargin

  /*
   * Query 11: Get all information about an offer.
   *
   * After deciding on a specific offer, the consumer wants to get all information
   * that is directly related to this offer.
   * */

  val query11 =
    """
     |SELECT *
     |FROM   SingleTable t1
     |WHERE  t1.subject =
     |'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1'
   """.stripMargin

  /*
   * Query 12: Export information about an offer into another schemata.
   *
   * After deciding on a specific offer, the consumer wants to save information
   * about this offer on his local machine using a different RDF schema.
   * */

  val query12 =
    """
      |SELECT Distinct t1.subject as productNr, t2.object as productlabel,
      |t4.object as offerURL, t5.object as price,
      |t6.object as deliveryDays, t7.object as validTo,
      |t9.object as vendorname, t10.object as vendorhomepage
      |FROM SingleTable t1, SingleTable t2, SingleTable t3,
      |SingleTable t4, SingleTable t5, SingleTable t6, SingleTable t7,
      |SingleTable t8, SingleTable t9, SingleTable t10
      |WHERE t1.subject = t2.subject AND t1.subject = trim(t3.object)
      |AND t2.predicate = 'http://www.w3.org/2000/01/rdf-schema#label'
      |AND t3.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/product'
      |AND t3.subject = t4.subject AND t3.subject = t5.subject
      |AND t3.subject = t6.subject AND t3.subject = t7.subject AND t7.subject = t8.subject
      |AND t4.subject = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1'
      |AND t4.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/offerWebpage'
      |AND t5.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/price'
      |AND t6.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/deliveryDays'
      |AND t7.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/validTo'
      |AND t8.predicate = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/vendor'
      |AND trim(t8.object) = t9.subject and t9.subject = t10.subject
      |AND t9.predicate = 'http://www.w3.org/2000/01/rdf-schema#label'
      |AND t10.predicate = 'http://xmlns.com/foaf/0.1/homepage'
    """.stripMargin

}
