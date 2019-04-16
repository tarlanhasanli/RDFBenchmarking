import CSVtoHDFS.HDFS_Converter;
import CSVtoHDFS.HDFS_type;
import RDF_XMLtoCSV.RDF_XML_Converter;

public class Main {


    public Main(){

        try {
            HDFS_Converter hdfs_converter =
                    new HDFS_Converter("/home/cloudera/RDFBenchmarking/Datasets/CSV");

            hdfs_converter.config()
                    .read()
                    .convert(HDFS_type.ORC)
                    .convert(HDFS_type.AVRO)
                    .convert(HDFS_type.PARQUET);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){

        new Main();

//        RDF_XML_Converter rdf_xml_converter = new RDF_XML_Converter();
//
//        rdf_xml_converter.read();
//        rdf_xml_converter.write();

    }


}
