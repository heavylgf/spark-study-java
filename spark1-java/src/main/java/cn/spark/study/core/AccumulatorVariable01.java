package cn.spark.study.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class AccumulatorVariable01 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Accumulator")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        final Accumulator<Integer> sum = sc.accumulator(0);

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        numbers.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer t) throws Exception {
                // 然后在函数内部，就可以对Accumulator变量，调用add()方法，累加值
                sum.add(t);
            }
        });

        // 在driver程序中，可以调用Accumulator的value()方法，获取其值
        System.out.println(sum.value());

        sc.close();

    }

}
