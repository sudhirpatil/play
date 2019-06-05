package com.sp.plalyground.helpers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PrimeNumbers {

    /**
     * based on  Sieve Sundaram  algorithm
     * Key Concepts : All prime numbers are odd except 2
     *  Odd numbers which are not produced by multiplying 2 odd numbers are prime numbers
     *
     *  i+j+2ij any number that cannot be represented as this
     * number to be prime (odd) number, it should not be != odd * odd
     * i.e Odd number but not prime = (2i+1)*(2j+1)
     *  = 2i + 2j + 4ij +1
     *  = 2(i+j+2ij)+1
     *  = 2(k)+1
     * So aim is to find out all k which cannot be represented as i+j+2ij
     *
     * Create array which indicates if index(k) can be represented as i+j+2ij & mark that index(k) true
     * Find out all indexe in array not marked as true & 2 * index + 1 = prime numbers
     *
     * https://luckytoilet.wordpress.com/2010/04/18/the-sieve-of-sundaram/
     *
     * @param limit
     * @return
     */
    public static List<Integer> getPrimeNumbers(int limit){
        int m = (int) (limit-2)/2; // ? why -2?
        int maxIndex = m+1;
        boolean[] primeIndicator = new boolean[maxIndex];
        Arrays.fill(primeIndicator, false);

        for (int i = 1;i<m;i++){
            for(int j=i;(i+j+(2*i*j)) <= m;j++){
                System.out.println("i:"+i+" j:"+j+" k:"+(i+j+(2*i*j)));
                primeIndicator[(i+j+(2*i*j))] = true;
            }
        }

        List<Integer> primeList = new ArrayList<>();
        primeList.add(2);
        for(int k =1; k<maxIndex;k++){
            if(primeIndicator[k] == false){
                primeList.add(2*k+1);
            }
        }

        for(int prime:  primeList) System.out.print(prime+" ");
        return primeList;
    }

    public static void main(String[] args) {
        getPrimeNumbers(50);
    }
}
