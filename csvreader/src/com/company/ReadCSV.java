package com.company;

import java.io.*;
import java.net.URI;
import java.util.*;

class ReadCSV {

    private String sourceFile;
    private String destinationPath;

    ReadCSV(String sourceFile, String destinationPath){
        this.sourceFile = sourceFile;
        this.destinationPath = destinationPath;
    }

    List readData() throws Exception {

        String file = sourceFile;
        List<String[]> content = new ArrayList<>();
        try(BufferedReader br = new BufferedReader(new FileReader(file))) {
            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                content.add(line.split(","));
            }
        } catch (Exception e) {
            throw new Exception("File not wound " + e);
        }

        return content;
    }

    boolean writeData(Hashtable<String, List<String[]>> hashTable){

       try {
            hashTable.forEach((k, v) -> createCSV(k, v));
        } catch (Exception e){
            e.printStackTrace();
            return false;
        }

        return true;

    }

    private void createCSV(String name, List<String []> content){

        File file = new File(destinationPath+"\\"+URItoFilename(name)+".csv");
        file.getParentFile().mkdirs();

        try {

            Writer writer = new FileWriter(file, false);

            writer.write("Subject,Object\n");

            for(String[] s : content){
                writer.write(s[0]+","+s[1]+"\n");
            }

            writer.close();
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }

    }

    String generateFilename(String uri){

        try{
            URI u = new URI(uri);
            File f = new File(u.getPath());
            return f.getName();
        } catch (Exception e){
            System.out.println("uri to file name error " + e);
            return null;
        }

    }

    String URItoFilename(String uri){

        try{
            uri = uri.substring(
                    uri.lastIndexOf("/")+1);
            uri = uri.substring(
                    uri.lastIndexOf("#")+1);
            return uri;
        } catch (Exception e){
            System.out.println("uri to file name error " + e);
            return null;
        }

    }

}
