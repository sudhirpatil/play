package com.sp.plalyground.practise.hackerrank.hashmap;

import java.io.*;
import java.util.*;

import static java.util.stream.Collectors.joining;

public class FrequencyQuery {

    // Complete the freqQuery function below.
    static List<Integer> freqQuery(int[][] queries) {
        List<Integer> frequencies = new ArrayList<>();
        Map<Integer, Integer> valueFrequency = new HashMap<>();
        Map<Integer, Integer> frequencyCount = new HashMap<>();

        for(int[] query : queries){
            Integer queryType = query[0];
            Integer queryValue = query[1];

            Integer count = 0;
            Integer reverseCount = 0;
            if(queryType == 1){
                //Update map count for queryValue with count+1
                valueFrequency.merge(queryValue, 1, Integer::sum);
                //ReverseMap :: increase count for current count
                count = valueFrequency.get(queryValue);
                frequencyCount.merge(count,1, Integer::sum);
                //reverseMap :: decrease count for prev count and delete key if count becomes 0
                reverseCount = frequencyCount.get(count -1);
                if(reverseCount != null){
                    if(reverseCount == 1) {
                        frequencyCount.remove(count - 1);
                    }else {
                        frequencyCount.put(count-1, reverseCount -1);
                    }
                }
            }else if(queryType == 2){
                count = valueFrequency.get(queryValue);
                if(count != null){
                    // Update map with count - 1 & delete if count -1 ==0
                    if(count == 1){
                        valueFrequency.remove(queryValue);
                    }else{
                        valueFrequency.put(queryValue, count -1);
                    }

                    // Increase the count for reverseMap prev count
                    if(count > 1){
                        frequencyCount.merge(count-1, 1, Integer::sum);
                    }
                    //ReverseMap :: decrease count for current count
                    reverseCount = frequencyCount.get(count);
                    if(reverseCount == 1){
                        frequencyCount.remove(count);
                    }else {
                        frequencyCount.put(count, reverseCount -1);
                    }

                }
                //Delete from map if any of the values are 0 in map & reverse map
            }else if(queryType == 3){
                // Check if count exists in reverse map & count >0 then add to frequency list
                frequencies.add(frequencyCount.get(queryValue) == null ? 0 : 1);
//                if(reverseMap.containsKey(queryValue)){
//                    frequencies.add(1);
//                }else {
//                    frequencies.add(0);
//                }
            }
        }

        return frequencies;

    }

    static List<Integer> freqQueryOld(List<List<Integer>> queries) {
        System.out.println(" In Method");
        List<Integer> frequencies = new ArrayList<>();
        Map<Integer, Integer> map = new HashMap<>();
        Map<Integer, Integer> reverseMap = new HashMap<>();

        for(List<Integer> query : queries){
            int queryType = query.get(0);
            int queryValue = query.get(1);

            int count = 0;
            int reverseCount = 0;
            if(queryType == 1){
                if(map.containsKey(queryValue)){
                    count = map.get(queryValue);
                    reverseMap.put(count, reverseMap.get(count) - 1);
                }
                count +=1;
                map.put(queryValue,count);
                if(reverseMap.containsKey(count)){
                    reverseCount = reverseMap.get(count);
                }
                reverseMap.put(count, reverseCount + 1);
                System.out.println("value:"+queryValue +" ,count:"+count);
            }else if(queryType == 2){
                if(map.containsKey(queryValue)){
                    count = map.get(queryValue);
                    reverseMap.put(count, reverseMap.get(count) -1);

                    count -= 1;
                    map.put(queryValue, count);
                    if(reverseMap.containsKey(count)){
                        reverseCount = reverseMap.get(count);
                    }
                    reverseMap.put(count, reverseCount + 1);
                    System.out.println("value:"+queryValue +" ,count:"+count);
                }
            }else if(queryType == 3){
                System.out.println(reverseMap.containsKey(queryValue));
                if(reverseMap.containsKey(queryValue) && reverseMap.get(queryValue) > 0){
                    frequencies.add(1);
                }else {
                    frequencies.add(0);
                }
            }
        }

        return frequencies;

    }

    public static void main(String[] args) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
//        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(System.getenv("OUTPUT_PATH")));

        int q = Integer.parseInt(bufferedReader.readLine().trim());

//        List<List<Integer>> queries = new ArrayList<>();
//
//        IntStream.range(0, q).forEach(i -> {
//            try {
//                queries.add(
//                        Stream.of(bufferedReader.readLine().replaceAll("\\s+$", "").split(" "))
//                                .map(Integer::parseInt)
//                                .collect(toList())
//                );
//            } catch (IOException ex) {
//                throw new RuntimeException(ex);
//            }
//        });

        int[][] queries = new int[q][2];

        for (int i = 0; i < q; i++) {
            String[] query = bufferedReader.readLine().split(" ");
            queries[i][0] = Integer.parseInt(query[0]);
            queries[i][1] = Integer.parseInt(query[1]);
        }

        List<Integer> ans = freqQuery(queries);
        System.out.println(ans);

//        bufferedWriter.write(
//                ans.stream()
//                        .map(Object::toString)
//                        .collect(joining("\n"))
//                        + "\n"
//        );

        bufferedReader.close();
//        bufferedWriter.close();
    }
}
