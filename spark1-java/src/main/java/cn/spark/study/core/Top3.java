package cn.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class Top3 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Top3")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("C://Users//Administrator//Desktop//top.txt");

        // 映射成一个（数字，字符串的格式）
        JavaPairRDD<Integer, String> pairs = lines.mapToPair(

                new PairFunction<String, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(String s) throws Exception {
                        return new Tuple2<Integer, String>(Integer.valueOf(s), s);
                    }
                });

        // 降序排队
        JavaPairRDD<Integer, String> sortedPairs = pairs.sortByKey(false);

        JavaRDD<Integer> sortedNumbers = sortedPairs.map(

                new Function<Tuple2<Integer,String>, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Tuple2<Integer, String> v1) throws Exception {
                        return v1._1;
                    }

                });


        // 取出前3个
         List<Integer> sortedNumberList = sortedNumbers.take(3);

         for(Integer number : sortedNumberList){
             System.out.println(number);
         }

         sc.close();

    }

}

