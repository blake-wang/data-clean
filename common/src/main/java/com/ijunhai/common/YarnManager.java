package com.ijunhai.common;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.HashSet;


public class YarnManager {

    public static void main(String[] args) {

    }

    public static  HashSet<String> get(HashSet<String> set, BufferedReader input) throws Exception {
        String line;
        while ((line = input.readLine()) != null) {
            if (!line.contains("pplication")) {
                set.add(line.trim());
                System.out.println(line.trim());
            }
        }
        return set;
    }

    public static Object exec(String cmd) {
        try {
            String[] cmdA = { "/bin/sh", "-c", cmd };
            Process process = Runtime.getRuntime().exec(cmdA);
            LineNumberReader br = new LineNumberReader(new InputStreamReader(
                    process.getInputStream()));
            StringBuffer sb = new StringBuffer();
            String line;
//            if ((line = br.readLine()) != null) {
//                System.out.println(line);
//                sb.append(line).append("\n");
//            }
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
