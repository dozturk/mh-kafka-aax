package com.durmus.kafka.mh.aax.client;

import com.durmus.kafka.mh.avro.aax.AAX;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AaxCsvParser {
    private List<String> headers;
    private String regex;
    private String inputPath;
    private String path;
    private static List<String> paths;
    private Integer nextFile;

    public AaxCsvParser(List<String> headers, String regex, String inputPath) {
        this.headers = headers;
        this.regex= regex;
        this.inputPath = inputPath;
    }

    private void init() throws Exception {

        try {
            File folder = new File(inputPath);
            File[] listOfFiles = folder.listFiles();
            if (listOfFiles.length > 0) {

                for (int i = 0; i < listOfFiles.length; i++) {
                    if (listOfFiles[i].isFile()){
                        //then get the list of absolute paths
//                        paths.add(inputPath + listOfFiles[i].getName());
//                        System.out.println(inputPath + listOfFiles[i].getName());
                    }
                }
//                nextFile = listOfFiles.length;
            }
            else {nextFile = null;}

        }catch (Exception e){
            nextFile = null;
            System.out.println("Exception while reading file ulan: "+ e);
        }


//        System.out.println(paths);
        path = "C:/Users/OzturkD/Desktop/aaa/proper.csv";
        nextFile = 1;

    }

    public List<AAX> getNextFile() throws Exception {
        if (nextFile == null) init();
        if (nextFile >= 1) {
            //  we fetch from the last file
//            nextFile -= 1;
//            List<AAX> result = readFile(paths.get(nextFile-1));
//            System.out.println(paths.get(nextFile-1));
            List<AAX> result = readFile(path);
            nextFile -= 1;
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    private List<AAX> readFile(String path) {

        Stream<String> rows = null;
        try {
            rows = Files.lines(Paths.get(path), StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<String[]> resultList = rows
                .skip(1)
                .map(x -> x.split(regex, -1))
                .collect(Collectors.toList());
        List<AAX> aaxes = this.convertResults(headers ,resultList);
        return  aaxes;
    }

    public List<AAX> convertResults(List<String> headers, List <String[]> resultsList){

        List<AAX> results = new ArrayList<>();
        for (int i = 0; i < resultsList.size(); i++) {
            AAX.Builder aaxBuilder = AAX.newBuilder();
            aaxBuilder.setId("101");
            AAX aax = aaxBuilder.build();
            List<String> data = Arrays.asList(resultsList.get(i));
            for (String datum : data ) {
//                System.out.println(headers.get(data.indexOf(datum))+ " - " + datum);
                aax.put(headers.get(data.indexOf(datum)), datum);
            }
            results.add(aax);
        }
        return results;
    }


    public void close() {

    }

}


