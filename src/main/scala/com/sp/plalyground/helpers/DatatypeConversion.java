package com.sp.plalyground.helpers;

public class DatatypeConversion {
    public void fromString(){
        // String -> Integer
        Integer.parseInt("100");    //returns 100
        // String in hex or binary -> Integer
        Integer.parseInt("-FF", 16);    //returns -255
        Integer.parseInt("1100110", 2); //returns 102

    }

    public void convertPrimitiveTypes(){
        // Float -> Int
        int i =  (int) 0.3;

        // int -> double
        double j = (double) i;
    }

    // Check if string is integer
    public static boolean isInteger(String str){
        try{
            Integer.parseInt(str);
            return true;
        }catch (Exception e){
            return false;
        }
    }

    public static boolean isIntegerOptimal(String str) {
        if (str == null) {
            return false;
        }
        int length = str.length();
        if (length == 0) {
            return false;
        }
        int i = 0;
        if (str.charAt(0) == '-') {
            if (length == 1) {
                return false;
            }
            i = 1;
        }
        for (; i < length; i++) {
            char c = str.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }

    public void charToInt(){
        // Add / Subtract char '0' to convert to int / char

        // int -> char
        char ch2 = (char)  ('0' + 2);

        // char -> int
        int chInt = '2' - '0';
    }
}
