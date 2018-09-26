package cn.spark.study.structuredstreaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by CTWLPC on 2018/9/17.
 * Structured Streaming：wordcount入门案例
 */
public class StructuredNetworkWordCount {

    public static void main(String[] args) throws StreamingQueryException {
        // 创建Spark上下文
        SparkSession spark  = SparkSession
                .builder()
                .appName("StructuredNetworkWordCount")
                .master("local[*]")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

//        Dataset<Row> df = spark.read().json("C:/Users/CTWLPC/Desktop/people.json");
//        // 显示DataFrame的内容
//        df.show();

        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        Dataset<String> words = lines.as(Encoders.STRING())
                .flatMap(new FlatMapFunction<String, String>() {

                    @Override
                    public Iterator<String> call(String line) throws Exception {
                        return Arrays.asList(line.split(" ")).iterator();

                    }
                }, Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();

    }

}
