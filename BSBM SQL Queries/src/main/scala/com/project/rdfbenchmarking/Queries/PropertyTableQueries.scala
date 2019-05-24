package com.project.rdfbenchmarking.Queries

class PropertyTableQueries {

  /*
  * Query 1: Find products for a given set of generic features.
  *
  * A consumer is looking for a product and has a general idea about what he wants.
  * */

  val query1: String =
    """
      |SELECT DISTINCT subject, label
      |FROM product p, productTypeProduct ptp
      |WHERE p.subject = ptp.product
      |AND ptp.productType = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1'
      |AND productPropertyNumeric1 > 600
      |AND p.subject IN
      | (SELECT DISTINCT product FROM productFeatureProduct WHERE productFeature = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature142')
      |AND p.subject IN
      | (SELECT DISTINCT product FROM productFeatureProduct WHERE productFeature = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature180')
      |ORDER BY label
      |LIMIT 10
    """.stripMargin

  /*
  * Query 2: Retrieve basic information about a specific product for display purposes
  *
  * The consumer wants to view basic information about products found by query 1.
  * */

  val query2: String =
  """
    |SELECT pt.label, pt.comment, pt.producer, productFeature, productPropertyTextual1,
    |productPropertyTextual2, productPropertyTextual3, productPropertyNumeric1,
    |productPropertyNumeric2, productPropertyTextual4, productPropertyTextual5,
    |productPropertyNumeric4
    |FROM product pt, producer pr, productFeatureProduct pfp
    |WHERE pt.subject = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1'
    |AND pt.subject = pfp.product
    |AND pt.producer = pr.subject
    """.stripMargin

  /*
  * Query 3: Find products having some specific features and not having one feature
  *
  * After looking at information about some products,
  * the consumer has a more specific idea what we wants.
  * Therefore, he asks for products having several features but not having a specific other feature.
  * */

  val query3: String =
  """
    |SELECT p.subject, p.label
    |FROM product p, productTypeProduct ptp
    |WHERE p.subject = ptp.product
    |AND productType = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1'
    |AND productPropertyNumeric1 > 600
    |AND productPropertyNumeric3 < 800
    |AND 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature142' IN (SELECT productFeature FROM productFeatureProduct WHERE product = p.subject)
    |AND 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature180' NOT IN (SELECT productFeature FROM productFeatureProduct WHERE product = p.subject)
    |ORDER BY p.label
    |LIMIT 10
    """.stripMargin

  /*
  * Query 4: Find products matching two different sets of features.
  *
  * After looking at information about some products,
  * the consumer has a more specific idea what we wants.
  * Therefore, he asks for products matching either one set of features or another set.
  * */

  val query4: String =
  """
    |SELECT distinct p.nr, p.label, p.productPropertyTextual1
    |FROM product p, productTypeProduct ptp
    |WHERE p.subject = ptp.product
    |AND ptp.productType = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1'
    |AND p.subject IN (SELECT distinct product FROM productFeatureProduct WHERE productFeature = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature142')
    |AND ((productPropertyNumeric1 > 600 AND p.subject IN (SELECT distinct product FROM productFeatureProduct WHERE productFeature = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature180')
    |) OR (productPropertyNumeric2 > 250 AND p.subject IN (SELECT distinct product FROM productFeatureProduct WHERE productFeature = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature178')))
    |ORDER BY label
    |LIMIT 10
    |OFFSET 5
    """.stripMargin

  /*
  * Query 5: Find product that are similar to a given product.
  *
  * The consumer has found a product that fulfills his requirements.
  * He now wants to find products with similar features.
  * */

  val query5: String =
  """
    |SELECT DISTINCT p.subject, p.label
    |FROM product p, product po,
    |   (Select distinct pfp1.product
    |   FROM productFeatureProduct pfp1,
    |     (SELECT productFeature FROM productFeatureProduct WHERE product = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1') pfp2
    |   WHERE pfp2.productFeature = pfp1.productFeature) pfp
    |WHERE p.subject = pfp.product
    |AND po.subject = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1'
    |AND p.subject != po.subject
    |AND p.productPropertyNumeric1 < (po.productPropertyNumeric1 + 120)
    |AND p.productPropertyNumeric1 > (po.productPropertyNumeric1 - 120)
    |AND p.productPropertyNumeric2 < (po.productPropertyNumeric2 + 170)
    |AND p.productPropertyNumeric2 > (po.productPropertyNumeric2 - 170)
    |ORDER BY label
    |LIMIT 5
    """.stripMargin

  /*
   * Query 6: Find products having a label that contains a specific string.
   *
   * The consumer remembers parts of a product name from former searches.
   * He wants to find the product again by searching for the parts of the name that he remembers.
   * */

  val query6: String =
    """
      |SELECT subject, label
      |FROM product p
      |WHERE label like '%manner%'
    """.stripMargin

  /*
  * Query 7: Retrieve in-depth information about a specific product including offers and reviews.
  *
  * The consumer has found a products which fulfills his requirements.
  * Now he wants in-depth information about this product including offers from
  * German vendors and product reviews if existent.
  * */

  val query7: String =
  """
    |SELECT *
    |FROM (select label from product where subject = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1') p LEFT JOIN
    | ((select o.subject as onr, o.price, v.subject as vnr, v.label from offer o, vendor v
    | where 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1' = o.product
    | AND o.vendor = v.subject
    | AND v.country = 'http://downlode.org/rdf/iso-3166/countries#DE'
    | AND o.validTo > '2008-03-26T00:00:00') ov RIGHT JOIN
    |(select r.subject as rnr, r.title, pn.subject as pnnr, pn.name, r.rating1, r.rating2 from review r, person pn where
    |r.product = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1' AND
    |r.person = pn.subject) rpn on (1=1)) on (1=1)
    """.stripMargin

  /*
  * Query 8: Give me recent reviews in English for a specific product.
  *
  * TThe consumer wants to read the 20 most recent
  * English language reviews about a specific product.
  *
  * Note: Filter AND r.language = 'en' removed due to lack of column in data model
  * */

  val query8: String =
  """
    |SELECT r.title, r.text, r.reviewDate, p.subject, p.name, r.rating1, r.rating2, r.rating3, r.rating4
    |FROM review r, person p
    |WHERE r.product = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1'
    |AND r.person = p.subject
    |ORDER BY r.reviewDate DESC
    |LIMIT 20
    """.stripMargin

  /*
   * Query 9: Get information about a reviewer.
   *
   * In order to decide whether to trust a review, the consumer asks for any
   * kind of information that is available about the reviewer.
   * */

  val query9: String =
    """
      |SELECT p.subject, p.name, p.mbox_sha1sum, p.country, r2.subject, r2.product, r2.title
      |FROM review r, person p, review r2
      |WHERE r.subject = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review1'
      |AND r.person = p.subject
      |AND r2.person = p.subject
    """.stripMargin

  /*
   * Query 10: Get offers for a given product which fulfill specific requirements.
   *
   * The consumer wants to buy from a vendor in the United States that is able to
   * deliver within 3 days and is looking for the cheapest offer that fulfills these requirements.
   * */

  val query10: String =
    """
      |SELECT distinct o.subject, o.price
      |FROM offer o, vendor v
      |WHERE o.product = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1'
      |AND o.deliveryDays <= 3
      |AND v.country = 'http://downlode.org/rdf/iso-3166/countries#US'
      |AND o.validTo > '2008-03-26T00:00:00'
      |AND o.vendor = v.subject
      |Order BY o.price
      |LIMIT 10
    """.stripMargin

  /*
   * Query 11: Get all information about an offer.
   *
   * After deciding on a specific offer, the consumer wants to get all information
   * that is directly related to this offer.
   *
   * Note: Data producer removed due to lack of column in data model
   * */

  val query11: String =
    """
      |Select product, vendor, price, validFrom, validTo, deliveryDays, offerWebpage, publisher, date
      |from offer
      |where subject = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1'
    """.stripMargin

  /*
   * Query 12: Export information about an offer into another schemata.
   *
   * After deciding on a specific offer, the consumer wants to save information
   * about this offer on his local machine using a different RDF schema.
   * */

  val query12: String =
    """
      |Select p.subject As productNr,
      |p.label As productlabel,
      |v.label As vendorname,
      |v.homepage As vendorhomepage,
      |o.offerWebpage As offerURL,
      |o.price As price,
      |o.deliveryDays As deliveryDays,
      |o.validTo As validTo
      |From offer o, product p, vendor v
      |Where o.subject = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1'
      |AND o.product = p.subject
      |AND o.vendor = v.subject
    """.stripMargin

}
