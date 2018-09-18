package cn.spark.study.structuredstreaming.kafka;

import kafka.serializer.StringDecoder;
import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by CTWLPC on 2018/9/17.
 * 测试kafka
 */
public class TestKafka {

    public static void main(String[] args) {

        String appName = "TestKafka";
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName(appName);

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        Map<String, String> kafkaOptions = new HashMap<String, String>();
        kafkaOptions.put("kafka.bootstrap.servers", "192.168.0.121:9092");

        Dataset<Row> stream = sparkSession
                .readStream()
                .format("kafka")
                .options(kafkaOptions)
                .option("subscribe", "topic")
                .option("startingOffsets", "earliest")
                .load();
//                .select("value").as(Encoders.BINARY())
//                .selectExpr("deserialize(value) as row")
//                .select("row.*");

        stream.printSchema();
        stream.show();

        stream.select("name").show();

        stream.select("name", "age" + 1).show();

//        stream.filter("age" > 21);

        stream.groupBy("age").count().show();

//        // 创建输入DStream
//        Map<String, String> kafkaParams = new HashMap<String, String>();
//        kafkaParams.put("metadata.broker.list",
//                "192.168.0.103:9092,192.168.0.104:9092");
//
//        Set<String> topics = new HashSet<String>();
//        topics.add("news-access");
//
//        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
//                jssc,
//                String.class,
//                String.class,
//                StringDecoder.class,
//                StringDecoder.class,
//                kafkaParams,
//                topics);

    }
}
