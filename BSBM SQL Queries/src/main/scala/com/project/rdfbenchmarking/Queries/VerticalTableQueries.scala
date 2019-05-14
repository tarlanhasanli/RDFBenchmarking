package com.project.rdfbenchmarking.Queries

class VerticalTableQueries {

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


  val query6 =
    """
      |SELECT subject, object AS label
      |FROM Label
      |WHERE object like '%manner%'
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
      |SELECT n.subject, n.object AS name, m.object AS mbox_sha1sum, c.object AS country
      |r.subject, r.object as reviewFor, t.object as title
      |FROM Name n, Mbox_sha1sum m, Country c, ReviewFor r, Title t
      |WHERE n.subject = m.subject
      |AND m.subject = c.subject
      |AND r.subject = t.subject
      |AND r.subject = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review1'
      |AND r.object = n.subject
    """.stripMargin

  /*
   * Query 10: Get offers for a given product which fulfill specific requirements.
   *
   * The consumer wants to buy from a vendor in the United States that is able to
   * deliver within 3 days and is looking for the cheapest offer that fulfills these requirements.
   * */

  val query10 =
    """
      |SELECT DISTINCT p.subject, p.object AS price
      |FROM Price p, Product pr, DeliveryDays dd, ValidTo vt, Country c, Vendor v
      |WHERE pr.subject = p.subject
      |AND dd.subject = p.subject
      |AND vt.subject = p.subject
      |AND v.subject = p.subject
      |AND p.object = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1'
      |AND SUBSTRING(dd.object,1,CHARINDEX('^',dd.object)-1)<=3
      |AND SUBSTRING(vt.object,1,CHARINDEX('^',vt.object)-1)>'2008-08-01T00:00:00'
      |AND v.object = c.subject
      |AND c.object = 'US'
      |ORDER BY p.object
      |LIMIT 10
    """.stripMargin

  /*
   * Query 11: Get all information about an offer.
   *
   * After deciding on a specific offer, the consumer wants to get all information
   * that is directly related to this offer.
   * */

  val query11 =
    """
      |SELECT pr.object AS product, v.object AS vendor, p.object AS price,
      |vf.object AS validFrom, vt.object AS validTo, dd.object AS deliveryDays,
      |ow.object AS offerWebpage
      |FROM Product pr, Vendor v, Price p, ValidFrom vf, ValidTo vt, DeliveryDays dd, OfferWebpage ow
      |WHERE pr.subject = v.subject
      |AND v.subject = p.subject
      |AND p.subject = vf.subject
      |AND vf.subject = vt.subject
      |AND vt.subject = dd.subject
      |AND dd.subject = ow.subject
      |AND ow.subject = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1'
    """.stripMargin

  /*
   * Query 12: Export information about an offer into another schemata.
   *
   * After deciding on a specific offer, the consumer wants to save information
   * about this offer on his local machine using a different RDF schema.
   * */

  val query12 =
    """
      |SELECT p.object AS productNr, lp.object as productLabel, vp.object = vendorName,
      |h.object = vendorHomepage
      |FROM Product p, Label lp, Label vp, Homepage h, OfferWebpage ow,
      |Price pr, DeliveryDays dd, ValidTo vt, Vendor v
      |WHERE p.subject = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1'
      |AND ow.subject = p.subject
      |AND ow.subject = pr.subject
      |AND pr.subject = dd.subject
      |AND dd.subject = vt.subject
      |AND vt.subject = v.subject
      |AND vp.subject = h.subject
      |AND h.subject = v.object
      |AND p.object = lp.subject
    """.stripMargin

}
