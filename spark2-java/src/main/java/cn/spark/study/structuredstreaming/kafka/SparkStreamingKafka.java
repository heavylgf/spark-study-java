package cn.spark.study.structuredstreaming.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.*;

/**
 * Created by CTWLPC on 2018/9/17.
 * 测试kafka
 */
public class SparkStreamingKafka {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
		conf.setMaster("local[*]");
        conf.setMaster("spark://192.168.101.204:7077");
        conf.setAppName("SparkStreamingKafka");
        // 是让streaming任务可以优雅的结束，当把它停止掉的时候，
        // 它会执行完当前正在执行的任务。
        conf.set("spark.streaming.stopGracefullyOnShutdown","true");
        conf.set("spark.default.parallelism", "6");

        // 创建上下文
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        // 这个是设置每一个批处理的时间，我这里是设置的10秒，通常可以1秒。。
        JavaStreamingContext jssc = new JavaStreamingContext(javaSparkContext, Durations.seconds(10));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers",
                "192.168.101.204:9092,192.168.101.205:9092,192.168.101.206:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "myGroup998");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);

        // 创建主题列表：
        Collection<String> topic0 = Arrays.asList("self-topic0");
        Collection<String> topic1 = Arrays.asList("self-topic1");
        Collection<String> topic2 = Arrays.asList("self-topic2");
        Collection<String> topic3 = Arrays.asList("self-topic3");
        Collection<String> topic4 = Arrays.asList("self-topic4");
        Collection<String> topic5 = Arrays.asList("self-topic5");
        List<Collection<String>> topics = Arrays.asList(topic0, topic1, topic2, topic3, topic4, topic5);
        List<JavaDStream<ConsumerRecord<String, String>>> kafkaStreams = new ArrayList<>(topics.size());

        // 主题列表本来是可以这样的：
        // 如果是这样的话，这多个主题会在一个消费者中去接收。而我上面的写法可以让spark更好的并发去处理每一个主题。
        // Collection<String> topic = Arrays.asList("self-topic0","self-topic1","self-topic2");

        // 最后，就是为每一个主题开一个stream去接收，收完了再把结果union起来。
        for (int i = 0; i < topics.size(); i++) {
            kafkaStreams.add(KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics.get(i), kafkaParams)));
        }

        JavaDStream<ConsumerRecord<String, String>> stream = jssc.union(kafkaStreams.get(0),
                kafkaStreams.subList(1, kafkaStreams.size()));

        stream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            rdd.foreachPartition(consumerRecords -> {
                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                System.out.println(
                        o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            });
        });

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }



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
//        Dataset<Row> stream = sparkSession
//                .readStream()
//                .format("kafka")
//                .options(kafkaOptions)
//                .option("subscribe", "topic")
//                .option("startingOffsets", "earliest")
//                .load();
//                .select("value").as(Encoders.BINARY())
//                .selectExpr("deserialize(value) as row")
//                .select("row.*");

    }
}
