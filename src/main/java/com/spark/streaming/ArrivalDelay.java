package com.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapTupleToRow;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.someColumns;
import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

/**
 * Created by shubham.kankaria on 14/02/16.
 */
public class ArrivalDelay implements Serializable {

    private static final Pattern SPACE = Pattern.compile(" ");

    public void run(String [] args) {
        if (args.length < 2) {
            System.err.println("Usage: ArrivalDelay <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        String brokers = args[1];
        String topics = args[2];

        SparkConf sparkConf = new SparkConf().setAppName("ArrivalDelayCount")
                .set("spark.cassandra.connection.host", "127.0.0.1");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });

        JavaDStream<List<String>> inKeyValue = lines.map(new Function<String, List<String>>() {
            @Override
            public List<String> call(String str) throws Exception {
                return Arrays.asList(str.split(","));
            }
        });

        JavaPairDStream<String, Double> outKeyValue = inKeyValue.mapToPair(new PairFunction<List<String>, String, Tuple2<Double, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<Double, Integer>> call(List<String> strings) throws Exception {
                return new Tuple2<>(strings.get(0),
                        new Tuple2<>(Double.parseDouble(strings.get(1)), 1));
            }
        }).reduceByKey(new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
            @Override
            public Tuple2<Double, Integer> call(Tuple2<Double, Integer> v1, Tuple2<Double, Integer> v2) throws Exception {
                return new Tuple2<>(v1._1.doubleValue() + v2._1().doubleValue(), v1._2().intValue() + v2._2().intValue());
            }
        }).mapValues(new Function<Tuple2<Double,Integer>, Double>() {
            @Override
            public Double call(Tuple2<Double, Integer> v1) throws Exception {
                return v1._1().doubleValue() / v1._2();
            }
        });

        JavaDStream<Tuple2<String, Double>> result = outKeyValue.transform(new Function<JavaPairRDD<String, Double>, JavaRDD<Tuple2<String, Double>>>() {
            @Override
            public JavaRDD<Tuple2<String, Double>> call(JavaPairRDD<String, Double> v1) throws Exception {
                return JavaRDD.fromRDD(JavaPairRDD.toRDD(v1), v1.classTag());
            }
        }).transform(new Function<JavaRDD<Tuple2<String, Double>>, JavaRDD<Tuple2<String, Double>>>() {
            @Override
            public JavaRDD<Tuple2<String, Double>> call(JavaRDD<Tuple2<String, Double>> v1) throws Exception {
                return v1.sortBy(new Function<Tuple2<String, Double>, Double>() {
                    @Override
                    public Double call(Tuple2<String, Double> v1) throws Exception {
                        return v1._2();
                    }
                }, true, 1);
            }
        });

        javaFunctions(result).writerBuilder("aviation", "airline_arrival", mapTupleToRow(
                String.class,
                Double.class
        )).withColumnSelector(someColumns("airline_id","arr_delay_minutes")).saveToCassandra();

        result.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
