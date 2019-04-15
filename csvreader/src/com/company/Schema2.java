package com.company;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class Schema2 {

    private String destinationPath;
    private String sourceFile;
    private ReadCSV read;

    public Schema2(String sourceFile, String destinationPath){

        this.sourceFile = sourceFile;
        this.destinationPath = destinationPath;

    }

    public boolean convert(){

        this.read = new ReadCSV(sourceFile, destinationPath);

        try {

            List content = read.readData();
            Hashtable data = groupDataPredicate(content);

            return read.writeData(data);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;

    }

    private Hashtable<String, List<String[]>> groupDataPredicate(List<String []> content){

        Hashtable<String, List<String []>> hashTable = new Hashtable<>();

        for(String[] items : content){
            String key = items[1];
            String [] value = {items[0], items[2]};

            if(hashTable.containsKey(key)){
                List<String[]> current = hashTable.get(key);
                current.add(value);

                hashTable.put(key, current);
            }else{
                List<String[]> current = new ArrayList<>();
                current.add(value);

                hashTable.put(key, current);
            }
        }

        return hashTable;

    }

}
