package com.spark.streaming;

/**
 * Created by shubham.kankaria on 14/02/16.
 */
public class Application {

    public static void main(String [] args) {
        if(args[0].equals("ArrivalDelay")) {
            new ArrivalDelay().run(args);
        }
    }
}
