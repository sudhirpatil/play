package com.sp.plalyground.helpers;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

public class ArrayDequeStackQueue {
    public static void dequeStack(List<Integer> list){
        ArrayDeque<Integer> deque = new ArrayDeque<>();
        // push , peek, pop
        // push to stack
        for(int item: list){
            deque.push(item);
        }
        // Element at top of stack, does not remove element
        System.out.println(deque.peek());
        // pop from stack, pop method throws NoSuchElementException when a stack is empty.
        while(!deque.isEmpty()){
            System.out.println(deque.pop());
        }
    }

    public static void dequeAsQueue(List<Integer> list){
        Deque<String> queue = new ArrayDeque<>();
        // offer , poll , peek
        queue.offer("first");
        queue.offer("second");

        while(!queue.isEmpty()){
            System.out.println(queue.poll());
        }
    }

    public static void main(String[] args) {
        List<Integer> list = Arrays.asList(2,4,9,2,1,89);
        dequeAsQueue(list);
    }
}
