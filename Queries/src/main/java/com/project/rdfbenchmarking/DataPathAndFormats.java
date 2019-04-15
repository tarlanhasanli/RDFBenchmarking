package com.project.rdfbenchmarking.queries;

public enum DataPathAndFormats {

    CSV_FORMAT("csv"),
    CSV_PATH("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/"),
    AVRO_FORMAT("com.databricks.spark.avro"),
    AVRO_PATH("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/"),
    ORC_FORMAT("org.apache.spark.sql.execution.datasources.orc"),
    ORC_PATH("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/ORC/"),
    PARQUET_FORMAT("parquet"),
    PARQUET_PATH("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/Parquet/");

    private String value;

    DataPathAndFormats(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return this.getValue();
    }

    public static DataPathAndFormats getEnum(String value) {

        if(value == null){
            throw new IllegalArgumentException();
        }

        for(DataPathAndFormats v : values()) {
            if (value.equalsIgnoreCase(v.getValue())){
                return v;
            }
        }

        throw new IllegalArgumentException();

    }

}
