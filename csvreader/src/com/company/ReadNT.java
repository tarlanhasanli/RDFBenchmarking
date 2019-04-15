package com.company;

import java.io.*;
import java.util.*;

class ReadNT {

    private List<String []> content;

    public ReadNT(){

        String[] header = {"subject", "predicate", "object"};

        content = new ArrayList<>();
        content.add(header);

    }

    public void readData(){

        try {

            BufferedReader br = new BufferedReader(new FileReader(
                    "C:\\Users\\Tarlan Hasanli\\Documents\\2019 spring\\Big Data Management\\bsbmtools-0.2\\dataset.nt"));

            String line;

            while ((line = br.readLine()) != null)
                content.add(line
                        .replaceAll("[<>]+","") // remove < and > signs around URIs
                        .trim() //remove extra spaces
                        .replaceFirst(".$","") // remove last character '.'
                        .split(" ", 3)); // split into subject, predicate, object

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void writeData(){

        StringBuilder sb = new StringBuilder();
        File file = new File(
                "C:\\Users\\Tarlan Hasanli\\Documents\\2019 spring\\Big Data Management\\bsbmtools-0.2\\newDataset.csv");

        content.forEach(row -> {
            sb.append(String.join(",", row)).append("\n");
        });

        try {

            FileWriter fw = new FileWriter(file, false);

            fw.write(sb.toString());

            fw.close();

        } catch (Exception e) {
            e.printStackTrace();
        }





    }

}
