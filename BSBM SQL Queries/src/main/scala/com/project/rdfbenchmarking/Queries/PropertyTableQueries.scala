package com.project.rdfbenchmarking.Queries

class PropertyTableQueries {

  /*
		 * Query 6: Find products having a label that contains a specific string.
		 *
		 * The consumer remembers parts of a product name from former searches.
		 * He wants to find the product again by searching for the parts of the name that he remembers.
		 * */

  val query6 =
    """
      |SELECT subject, label
      |FROM product p
      |WHERE label like '%manner%'
    """.stripMargin

  /*
		 * Query 9: Get information about a reviewer.
		 *
		 * In order to decide whether to trust a review, the consumer asks for any
		 * kind of information that is available about the reviewer.
		 * */

  val query9 =
    """
      |SELECT p.subject, p.name, p.mbox_sha1sum, p.country, r2.subject, r2.reviewFor, r2.title
      |FROM review r, person p, review r2
      |WHERE r.subject = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review1'
      |AND trim(r.reviewer)=p.subject AND trim(r2.reviewer)=p.subject
    """.stripMargin

  /*
		 * Query 10: Get offers for a given product which fulfill specific requirements.
		 *
		 * The consumer wants to buy from a vendor in the United States that is able to
		 * deliver within 3 days and is looking for the cheapest offer that fulfills these requirements.
		 * */

  val query10 =
    """
      |SELECT distinct o.subject, SUBSTRING(o.price ,1,instr(o.price , '^')-1) AS price
      |FROM offer o, vendor v
      |WHERE trim(o.product) = 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1'
      |AND SUBSTRING(o.deliveryDays,1,instr(o.deliveryDays,'^')-1)<=3
      |AND trim(v.country) = 'http://downlode.org/rdf/iso-3166/countries#GB'
      |AND CAST(SUBSTRING(o.validTo,1,instr(o.validTo,'^')-1) AS DATE) > CAST('2008-05-01T00:00:00' AS DATE)
      |AND trim(o.vendor) = v.subject
      |Order BY price
      |LIMIT 10
    """.stripMargin

  /*
		 * Query 11: Get all information about an offer.
		 *
		 * After deciding on a specific offer, the consumer wants to get all information
		 * that is directly related to this offer.
		 * */

  // TODO Add Following columns to below query: producer, publisher, publishDate
  val query11 =
    """
      |SELECT product, vendor, price, validFrom, validTo, deliveryDays, offerWebpage
      |FROM offer
      |WHERE subject='http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1'
    """.stripMargin

  /*
		 * Query 12: Export information about an offer into another schemata.
		 *
		 * After deciding on a specific offer, the consumer wants to save information
		 * about this offer on his local machine using a different RDF schema.
		 * */

  val query12 =
    """
      |Select p.subject As productNr, p.label As productlabel, v.label As vendorname, v.homepage As vendorhomepage,
      |o.offerWebpage As offerURL, o.price As price, o.deliveryDays As deliveryDays, o.validTo As validTo
      |From offer o, product p, vendor v
      |Where o.subject='http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1'
      |AND trim(o.product)=p.subject AND trim(o.vendor)=v.subject
    """.stripMargin

}
