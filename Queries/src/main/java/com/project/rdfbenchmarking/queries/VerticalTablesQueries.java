package com.project.rdfbenchmarking.queries;

public enum VerticalTablesQueries {

    QUERY6("SELECT subject, label" +
            "FROM product p" +
            "WHERE label like '%manner%';"),
    QUERY9("SELECT p.subject, p.name, p.mbox_sha1sum, p.country, r2.subject, r2.reviewFor, r2.title"+
           "FROM review r, person p, review r2"+
           "WHERE r.nr='http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review1' " +
            "AND r.reviewer=p.subject AND r2.reviewer=p.subject;"),
    QUERY10("SELECT distinct o.subject, o.price" +
            "FROM offer o, vendor v" +
            "WHERE o.product='http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product1'" +
            "AND SUBSTRING(o.deliveryDays,1,CHARINDEX('^',o.deliveryDays)-1)<=3 " +
            "AND v.country='US'" +
            "AND SUBSTRING(o.validTo,1,CHARINDEX('^',o.validTo)-1)>'2008-08-01T00:00:00' " +
            "AND o.vendor=v.subject" +
            "Order BY o.price" +
            "LIMIT 10;"),
    QUERY11("Select product, vendor, price, validFrom, validTo, " +
            "deliveryDays, offerWebpage, " +
//            "producer, publisher, publishDate" +
            "from offer" +
            "where subject='http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1';"),
    QUERY12("Select p.subject As productNr, p.label As productlabel, v.label As vendorname, v.homepage As vendorhomepage," +
            "o.offerWebpage As offerURL, o.price As price, o.deliveryDays As deliveryDays, o.validTo As validTo" +
            "From offer o, product p, vendor v" +
            "Where o.subject='http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1'" +
            "AND o.product=p.subject AND o.vendor=v.subject;");



    private String value;

    VerticalTablesQueries(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return this.getValue();
    }

    public static VerticalTablesQueries getEnum(String value) {

        if(value == null){
            throw new IllegalArgumentException();
        }

        for(VerticalTablesQueries v : values()) {
            if (value.equalsIgnoreCase(v.getValue())){
                return v;
            }
        }

        throw new IllegalArgumentException();

    }

}
