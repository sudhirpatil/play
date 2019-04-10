package com.sp.plalyground.hackerrank.hashmap;

import java.io.*;
import java.math.*;
import java.security.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

public class HashRansomNote {

    // Complete the checkMagazine function below.
    static void checkMagazine(String[] magazine, String[] note) {
        Map<String, Integer> magazineMap = new HashMap<>();
        for(String magazineStr : magazine) {
            Integer count = 1;
            if(magazineMap.containsKey(magazineStr)){
                count = magazineMap.get(magazineStr) + 1;
            }
            magazineMap.put(magazineStr, count);
        }

        Boolean isSubset = true;
        for(String noteStr : note){
            if(!magazineMap.containsKey(noteStr)){
                isSubset = false;
                break;
            }else{
                int currentCount = magazineMap.get(noteStr) - 1;
                if(currentCount == 0) {
                    magazineMap.remove(noteStr);
                }else{
                    magazineMap.put(noteStr, currentCount);
                }
            }
        }

        if(isSubset){
            System.out.println("Yes");
        }else {
            System.out.println("No");
        }
    }

    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        String[] mn = scanner.nextLine().split(" ");

        int m = Integer.parseInt(mn[0]);

        int n = Integer.parseInt(mn[1]);

        String[] magazine = new String[m];

        String[] magazineItems = scanner.nextLine().split(" ");
        scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");

        for (int i = 0; i < m; i++) {
            String magazineItem = magazineItems[i];
            magazine[i] = magazineItem;
        }

        String[] note = new String[n];

        String[] noteItems = scanner.nextLine().split(" ");
        scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");

        for (int i = 0; i < n; i++) {
            String noteItem = noteItems[i];
            note[i] = noteItem;
        }

        checkMagazine(magazine, note);

        scanner.close();
    }
}
