package cn.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * action操作实战
 */
public class ActionOperation01 {

    public static void main(String[] args) {
//        reduce();
//        collect();
//        count();
        take();

    }


    private static void reduce(){

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("reduce");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        int sum = numbers.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(sum);

        sc.close();
    }

    private static void collect(){

        SparkConf conf = new SparkConf()
                .setAppName("collect")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        JavaRDD<Integer> doubleNumbers = numbers.map(

                new Function<Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1) throws Exception {
                        return v1 * 2;
                    }
                });

        List<Integer> doubleNumberList = doubleNumbers.collect();
        for (Integer num: doubleNumberList){
            System.out.println(num);
        }

        sc.close();

    }

    private static void count(){

        SparkConf conf = new SparkConf()
                .setAppName("count")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        long count = numbers.count();

        System.out.println(count);

        sc.close();

    }

    private static void take(){

        SparkConf conf = new SparkConf()
                .setAppName("count")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        List<Integer> top2Numbers = numbers.take(2);

        for (Integer num : top2Numbers){
            System.out.println(num);
        }

        sc.close();

    }

    private static void saveAsTextFile(){

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("saveAsTextFile");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        JavaRDD<Integer> doubleNumbers = numbers.map(

                new Function<Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1) throws Exception {
                        return v1 * 2;
                    }
                });

        doubleNumbers.saveAsTextFile("hdfs://spark1:9000/double_number.txt");

        sc.close();

    }

    /**
     * 对每个key对应的value进行count
     */
    private static void countByKey(){

        // 创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("countByKey")
                .setMaster("local");
        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, String>> scoreList = Arrays.asList(
                new Tuple2<String, String>("class1", "leo"),
                new Tuple2<String, String>("class2", "jack"),
                new Tuple2<String, String>("class1", "marry"),
                new Tuple2<String, String>("class2", "tom"),
                new Tuple2<String, String>("class2", "david")
        );

        JavaPairRDD<String, String> students = sc.parallelizePairs(scoreList);

        Map<String, Object> studentCounts = students.countByKey();

        for (Map.Entry<String, Object> studentCount : studentCounts.entrySet()) {

            System.out.println(studentCount.getKey() + ": " + studentCount.getValue());

        }

        sc.close();

    }




}
