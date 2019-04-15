package com.company;

import java.io.*;
import java.util.*;

public class Schema3 {

    private String destinationPath;
    private String sourceFile;
    private ReadCSV read;
    private Hashtable<String, List<String>> dataModel;

    public Schema3(String sourceFile, String destinationPath){

        this.readDataModel();
        this.sourceFile = sourceFile;
        this.destinationPath = destinationPath;

    }

    private void readDataModel(){

        dataModel = new Hashtable<>();

        dataModel.put("Producer", new ArrayList<>(Arrays.asList("label","comment","type","homepage","country")));
        dataModel.put("ProductType", new ArrayList<>(Arrays.asList("label","comment","type","subClassOf")));
        dataModel.put("ProductFeature", new ArrayList<>(Arrays.asList("label","comment","type")));
        dataModel.put("Product", new ArrayList<>(Arrays.asList("label","comment","type","producer","productFeature",
                "productPropertyTextual1","productPropertyTextual2","productPropertyTextual3","productPropertyTextual4","productPropertyTextual5",
                "productPropertyNumeric1","productPropertyNumeric2","productPropertyNumeric3","productPropertyNumeric4","productPropertyNumeric5")));
        dataModel.put("Offer",  new ArrayList<>(Arrays.asList("product","vendor","price","validFrom","validTo","deliveryDays","offerWebpage")));
        dataModel.put("Review", new ArrayList<>(Arrays.asList("reviewFor","reviewer","reviewDate","title","text","rating1","rating2","rating3","rating4")));
        dataModel.put("Vendor", new ArrayList<>(Arrays.asList("label","comment","type","homepage","country")));
        dataModel.put("Reviewer", new ArrayList<>(Arrays.asList("name","mbox_sha1sum","country")));

    }

    public boolean convert(){

        this.read = new ReadCSV(sourceFile, destinationPath);

        try {

            List content = read.readData();
            Hashtable<String, List<String[]>> data = groupDataSubject(content);

            data.forEach((k, v) -> {

                Hashtable<String, String[]> table = createVerticalTable(k, v);

                createCSV(k, table);

            });

        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;

    }

    private Hashtable<String, List<String[]>> groupDataSubject(List<String []> content){

        Hashtable<String, List<String []>> hashTable = new Hashtable<>();

        for(String[] items : content){

            String key = items[0];
            int start=key.lastIndexOf("/");
            key = key.substring(start+1).replaceAll("\\d", "");


            String [] value = {items[0], items[1], items[2]};
            List<String[]> current;

            if(hashTable.containsKey(key))
                current = hashTable.get(key);
            else
                current = new ArrayList<>();

            current.add(value);

            hashTable.put(key, current);

        }

        return hashTable;

    }

    private Hashtable<String, String[]> createVerticalTable(String name, List<String []> content){

        Hashtable<String, String []> data = new Hashtable<>();
        List<String> list = dataModel.get(name);
        String[] columns = list.toArray(new String[0]);

        data.put("subject", columns);

        for (String[] row : content) {

            int index = Arrays.asList(columns).indexOf(read.URItoFilename(row[1]));
            if(index >= 0){
                String[] temp = (data.containsKey(row[0])) ? data.get(row[0]) : new String[columns.length];
                temp[index] = row[2];
                data.put(row[0], temp);
            }
        }

        return data;

    }

    private void createCSV(String name, Hashtable<String, String []> data){

        StringBuilder sb = new StringBuilder();

        sb.append("subject")
                .append(",")
                .append(String.join(",", data.get("subject")))
                .append("\n");

        data.remove("subject");

        data.forEach((k,v) -> {

            for(int i = 0; i < v.length; i++)
                if(v[i] == null)
                    v[i] = "";

            sb.append(k)
                    .append(",")
                    .append(String.join(",", v))
                    .append('\n');

        });

        File file = new File(destinationPath+"\\"+name+".csv");
        file.getParentFile().mkdirs();

        try {

            FileWriter writer = new FileWriter(file, false);
            writer.write(sb.toString());
            writer.close();

        } catch (IOException e){
            e.printStackTrace();
        }



    }


}
