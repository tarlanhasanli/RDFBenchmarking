import CSVtoAvro.Avro_Converter;
import CSVtoParquet.ParquetConverter;
import NTtoCSV.NT_Converter;

public class Main {


    public Main(){

        //new NT_Converter().read().write();

        //new Avro_Converter();

        new ParquetConverter();
    }

    public static void main(String[] args){

        new Main();

    }


}
