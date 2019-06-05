package com.sp.plalyground.helpers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegularExpressions {

    public static void patternObject(){
        Pattern pattern = Pattern.compile("\\bcat\\b");
        Matcher matcher = pattern.matcher("cat cat cat cattie cat");   // get a matcher object
        int count = 0;

        System.out.println(matcher.matches());
        while(matcher.find()) {
            count++;
            System.out.println("Match number "+count);
            System.out.println("start(): "+matcher.start());
            System.out.println("end(): "+matcher.end());
        }

        // now create a new pattern and matcher to replace whitespace with tabs
        Pattern replace = Pattern.compile("\\s+");
        Matcher matcher2 = replace.matcher("ss yy zz");
        System.out.println(matcher2.replaceAll("\t"));

    }

    public static void stringMatcher(){
        // Using String Matches
        boolean  prefixMatch = "123lll".matches("^\\d{3}.*");
        System.out.println(prefixMatch);

        // negative look ahead i.e a(?!b) a is not follwed by b
        System.out.println("axbxyz".matches("a(?!b).*"));

        // ?: To not to store grouped match in memory, useful when you want to group for repeat pattern match but don't want keep matches in memory
        System.out.println("abcxyz".matches("a(?:bc){2}.*"));

        // Removes whitespace between a word character and . or ,
        String pattern = "(\\w)(\\s+)([\\.,])";
        System.out.println("xyz   hhhh".replaceAll(pattern, "$1$3"));
    }

    public static void main(String[] args) {
        stringMatcher();

    }
}
