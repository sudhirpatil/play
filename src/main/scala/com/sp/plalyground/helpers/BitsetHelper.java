package com.sp.plalyground.helpers;

import java.util.BitSet;

public class BitsetHelper {
    public static void main(String args[]) {
        // Bitset Special array which holds bits, supports various bit operations like and,xor etc (adv over array)
        BitSet bits1 = new BitSet(16);
        BitSet bits2 = new BitSet(16);

        // set some bits
        for(int i = 0; i < 16; i++) {
            if((i % 2) == 0) bits1.set(i);
            if((i % 5) != 0) bits2.set(i);
        }

        System.out.println(bits1);
        // Number of bits set with 1
        System.out.println(bits1.cardinality());
        // AND bits
        bits2.and(bits1); //{2, 4, 6, 8, 12, 14}
        // OR bits
        bits2.or(bits1); //{0, 2, 4, 6, 8, 10, 12, 14}
        // XOR bits
        bits2.xor(bits1); //{}

        // Uses long[] internally
        // supports only keys with int
        // Not good if you want to store only few values here and there
    }
}
