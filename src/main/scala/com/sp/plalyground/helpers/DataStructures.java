package com.sp.plalyground.helpers;

import java.util.*;
import java.util.stream.Collectors;

public class DataStructures {
    public void DatatypeConversion(){
        int aInt =0;

        // int -> char
        char c=(char)aInt;

        // float/double to int by rounding lower
        int roundedInt = (int) 11/2;

        // converting operation on int to double
        double result = ((double) 1+ 2)/2;


    }

    public void arrayHelper(){
        // Instantiate array with specific length
        int[] maxArr = new int[10];
        // Fill array with 0's
        Arrays.fill(maxArr, 0);

        // Convert array to List of Integer
        int[] intArr = {1, 2, 3, 4, 5, 6};
        List<Integer> intList = new ArrayList<Integer>();
        for (int i : intArr)
        {
            intList.add(i);
        }
        // java 8 : functional way
        List<Integer> list = Arrays.stream(intArr)		// IntStream
                .boxed() 		// Stream<Integer>
                .collect(Collectors.toList());

        // Sorting
        Arrays.sort(maxArr); // Sorting array of primitive type
        // Sorting Objects, using comparator
//        Arrays.sort(T[]a , Comparator<T> b);

        // Get copy of sub array
        int[] subArray = Arrays.copyOfRange(intArr, 2,3 + 1);

        // print array contents
        Arrays.toString(intArr);
        // print nested string array
        String[][] nested = {{"row1col1", "row1col2"}, {"row2col1", "row2col2"}};
        Arrays.deepToString(nested);
    }

    public void helpList(){
        // List is interface, instantiate with ArrayList etc
        List<Integer> list = new ArrayList();
        // Initializing List
        List<String> iList = Arrays.asList("one", "two", "three");
        // Only in JDK 9
//        List<String> initializedList2 = List.of("one", "two", "three");

        // Iterate list
        for(String item : iList){
            System.out.println(item);
        }
        iList.forEach(item -> {
            System.out.println(item);
        });

        iList.add(2, "test"); // add element at index 2 and shifts if any existing elements
        iList.replaceAll(value -> value+"new");

        iList.sort((a, b) -> a.compareTo(b));
        Collections.max(iList);
        //Functional way to iterate
        iList.stream().
                filter(item -> item.contains("x")).
                forEach(System.out::println);

    }


    public void mapHelp(){
        Map<String, Integer> map = new HashMap<>();
        map.put("Larry", 1);
        map.put("Steve", 2);
        map.put("James", 3);

        // Iterate, if needed to modify external variable
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + "/" + entry.getValue());
        }

        // Iterate functional way, but cannot modify external variables
        map.
                forEach((key, value) -> System.out.println(key + " " + value));
        map.entrySet().
                forEach(entry -> System.out.println(entry.getKey() + " " + entry.getValue()));


        // Put value if key doesn't exist
        map.putIfAbsent("Darwin", 6);
        // Compute value if key doesn't exist, new String() can be any method
        map.computeIfAbsent("test", k -> new Integer(2));

        // Increment map value
        map.merge("test",1, Integer::sum);
    }


    public void ArrayDequeHelper(){
        // Resizable array with add/delete from both sides of queue
        // ArrayDeque class is likely to be faster than Stack when used as a stack.
        // ArrayDeque class is likely to be faster than LinkedList when used as a queue.

        // Intializing an deque
        Deque<Integer> deque = new ArrayDeque<Integer>(10);
        // add() method to insert
        deque.add(10);
        deque.add(20);
        for (Integer element : deque)
        {
            System.out.println("Element : " + element);
        }

        // addFirst() method to insert at start
        deque.addFirst(564);
        // addLast() method to insert at end
        deque.addLast(24);
        // element() method : to get Head element
        System.out.println("\nHead Element using element(): " +
                deque.element());
        // getFirst() method : to get Head element
        System.out.println("Head Element using getFirst(): " +
                deque.getFirst());
        // getLast() method : to get last element
        System.out.println("Last Element using getLast(): " +
                deque.getLast());
    }

	public static void main(String[] args){

    }

}
