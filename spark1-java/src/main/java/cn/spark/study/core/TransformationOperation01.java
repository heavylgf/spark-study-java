package cn.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext$;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.codehaus.janino.Java;

import java.util.Arrays;
import java.util.List;

public class TransformationOperation01 {

    public static void main(String[] args) {
        map();
        filter();
        flatMap();

    }


    // map 算子案例，将集合中每个元素都乘以2
    private static void map(){

        // 创建SparkConf
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("map");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);


        JavaRDD<Integer> multipleNumberRDD = numberRDD.map(
                new Function<Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1) throws Exception {
                        return v1 * 2;
                    }
                });

        multipleNumberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer t) throws Exception {
                System.out.println(t);

            }
        });

        sc.close();
    }

    private static void filter(){
        // 创建SparkConf
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("filter");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        JavaRDD<Integer> evennumberRDD = numberRDD.filter(

                new Function<Integer, Boolean>() {
                    @Override
                    public Boolean call(Integer v1) throws Exception {
                        return v1 % 2 == 0 ;
                    }
                });

        evennumberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer t) throws Exception {
                System.out.println(t);
            }
        });

        sc.close();

    }

    private static void flatMap(){
        SparkConf conf = new SparkConf().setAppName("").setMaster("");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> lineList = Arrays.asList("hello you", "hello me", "hello world");

        JavaRDD<String> lines = sc.parallelize(lineList);

        JavaRDD<String> words = lines.flatMap(

                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String line) throws Exception {
                        return Arrays.asList(line.split(" "));
                    }
                });

        words.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();










    }




}
