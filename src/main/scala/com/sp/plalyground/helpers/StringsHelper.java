package com.sp.plalyground.helpers;

public class StringsHelper {
    public void stringMethods(){
        String str = "xyzabc";

        // check if it starts with xy
        str.startsWith("xy");

        // sub strings
        str.substring(0, 4); // endIndex in exclusive i.e index 4 is not included

        str.charAt(1); // Character at specific index

    }

    public void charMethods(){
        Character.isLetterOrDigit('0');  // To detect number characters

    }
}
