package com.sp.plalyground.practise.hackerrank.hashmap;

import java.io.*;
import java.util.*;

public class SherlockAnagram {

    // Complete the sherlockAndAnagrams function below.
    static int sherlockAndAnagrams(String s) {
        int anagramCount = 0;
        Map<String, Integer> subMap = new HashMap<>();

        for(int i=0; i< s.length(); i++){
            for(int j=i+1; j<= s.length(); j++){
                String subStr = s.substring(i,j);
//                System.out.println("orig:"+subStr+" anagramCount:"+anagramCount+" i:"+i+" j:"+j);

                // Get sorted string
                int subLen = subStr.length();
                char[] sorted = subStr.toCharArray();
                for(int subi=0; subLen > 1 && subi < subLen; subi++){

                    int index = subi;
                    for(int subj = subi +1; subj < subLen; subj++){
                        if(sorted[subi] > sorted[subj]){
                            char ch = sorted[subi];
                            sorted[subi] = sorted[subj];
                            sorted[subj] = ch;
                        }
                    }
                }

                String sortedStr = new String(sorted);

                if(subMap.containsKey(sortedStr)){
                    int val = subMap.get(sortedStr);
                    anagramCount += val;
                    System.out.println("orig:"+subStr+ "  sorted:"+ sortedStr+" anagramCount:"+anagramCount+" i:"+i+" j:"+j);
                    subMap.put(sortedStr, val + 1);
                }else{
                    subMap.put(sortedStr, 1);
                }

            }
        }

        return anagramCount;
    }

    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) throws IOException {
//        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(System.getenv("OUTPUT_PATH")));

        int q = scanner.nextInt();
        scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");

        for (int qItr = 0; qItr < q; qItr++) {
            String s = scanner.nextLine();

            int result = sherlockAndAnagrams(s);
            System.out.println("result: "+result);

//            bufferedWriter.write(String.valueOf(result));
//            bufferedWriter.newLine();
        }

//        bufferedWriter.close();

        scanner.close();
    }
}