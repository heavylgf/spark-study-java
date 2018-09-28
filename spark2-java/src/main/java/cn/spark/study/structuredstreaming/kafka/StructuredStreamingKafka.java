package cn.spark.study.structuredstreaming.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * Created by CTWLPC on 2018/9/26.
 */
public class StructuredStreamingKafka {

    public static void main(String[] args) throws StreamingQueryException {
        // 创建Spark上下文
        SparkSession spark = SparkSession
                .builder()
                .appName("StructuredStreamingKafka")
                .master("local[*]")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        // Creating a Kafka Source for Streaming Queries
        // Subscribe to 1 topic
        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers",
                        "192.168.101.204:9092,192.168.101.205:9092,192.168.101.206:9092")
                // subscribe 用于指定要消费的 topic
                .option("subscribe", "TestTopic")
                .load();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = lines
                .writeStream()
                .outputMode(OutputMode.Append())
                .format("console")
                .start();

        query.awaitTermination();

        // Start running the query that prints the running counts to the console
//        StreamingQuery query = lines.as(Encoders.STRING())
//                .writeStream()
//                .outputMode(OutputMode.Append())
//                .format("console")
//                .start();
//
//        query.awaitTermination();


//        Dataset<Row> line = lines.as(Encoders.STRING()).select("value").alias("word");
//
//        StreamingQuery query = line.writeStream()
//                .outputMode(OutputMode.Append())
//                .format("console")
//                .start();
//
//        query.awaitTermination();

////
////        Dataset<Row> wordCounts = line.groupBy("word").count();
////
////        StreamingQuery query = wordCounts.writeStream()
////                .outputMode("complete")
////                .format("console")
////                .start();
////
////        query.awaitTermination();
//
////
////        line.flatMap(new FlatMapFunction<Row, String>() {
////            @Override
////            public Iterator<String> call(Row row) throws Exception {
////                return Arrays.asList(row.split(" ")).iterator();;
////            }
////        });
////
////

//        Dataset<String> words = lines.as(Encoders.STRING())
//                .flatMap(new FlatMapFunction<String, String>() {
//
//                    @Override
//                    public Iterator<String> call(String line) throws Exception {
//                        return Arrays.asList(line.split(" ")).iterator();
//                    }
//                }, Encoders.STRING());
//
//        Dataset<Row> wordCounts = words.groupBy("value").count();
//
//        // Start running the query that prints the running counts to the console
//        StreamingQuery query = wordCounts.writeStream()
//                .outputMode(OutputMode.Append())
//                .format("console")
//                .start();
//
//        query.awaitTermination();

    }

}
