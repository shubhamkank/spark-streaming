package com.spark.streaming;

/**
 * Created by shubham.kankaria on 14/02/16.
 */
public class ArrivalDelay {

    public void run(String [] args) {
        if (args.length < 2) {
            System.err.println("Usage: ArrivalDelay <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        String brokers = args[0];
        String topics = args[1];

    }
}
