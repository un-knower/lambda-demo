package com.datacenter.lambda.speedlayer;

import com.alibaba.fastjson.JSONObject;
import com.datacenter.lambda.common.DimensionDateTime;
import com.datacenter.lambda.common.register.RegisterDaysBucketSpeedView;
import com.datacenter.lambda.common.register.RegisterHoursBucketSpeedView;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import static com.datacenter.lambda.speedlayer.JsonDimensionExtracter.*;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;


/**
 * Created by zuoc on 2017/7/20.
 */
public class RegisterSpeed {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("RegisterSpeed");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

        // 定义 cassandra 批视图
        final CassandraConnector cassandraConnector = CassandraConnector.apply(streamingContext.sparkContext().sc());
        try (final Session session = cassandraConnector.openSession()) {
            session.execute("USE demo");

            session.execute("CREATE TABLE IF NOT EXISTS register_days_speed_view (" +
                    "    dimension varchar," +
                    "    dimension_value varchar," +
                    "    days_bucket int," +
                    "    amount counter," +
                    "    PRIMARY KEY ((dimension, dimension_value), days_bucket)" +
                    ");");

            session.execute("CREATE TABLE IF NOT EXISTS register_hours_speed_view (" +
                    "    dimension varchar," +
                    "    dimension_value varchar," +
                    "    hours_bucket int," +
                    "    amount counter," +
                    "    PRIMARY KEY ((dimension, dimension_value), hours_bucket)" +
                    ");");
        }

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "192.168.1.248:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "register_spark_streaming");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        // 接收 kafka 流数据
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(Arrays.asList("tiny-rlogs-9"), kafkaParams)
                );


        // 转换和计算 kafka 流数据
        JavaPairDStream<DimensionDateTime, Integer> dimensionDateTimeStream = stream
                .transformToPair(rdd ->
                        rdd.flatMapToPair(record -> {
                            final JSONObject data = JSONObject.parseObject(record.value());
                            if (data.containsKey("ids")) {
                                final int registerAmount = data.getObject("ids", List.class).size();
                                return Arrays.asList(
                                        new Tuple2<DimensionDateTime, Integer>(extractOperatorDimensionDateTime(data), registerAmount),
                                        new Tuple2<DimensionDateTime, Integer>(extractAllDimensionDateTime(data), registerAmount),
                                        new Tuple2<DimensionDateTime, Integer>(extractAppDimensionDateTime(data), registerAmount),
                                        new Tuple2<DimensionDateTime, Integer>(extractServerDimensionDateTime(data), registerAmount)
                                ).iterator();
                            }
                            return Collections.emptyIterator();
                        })
                )
                .filter(tuple2 -> tuple2._2() > 0)
                .reduceByKey(((v1, v2) -> v1 + v2))
                .cache();

        // 计算每日注册实时视图
        dimensionDateTimeStream
                .map(tuple2 -> RegisterDaysBucketSpeedView.valueOf(tuple2._1().toDimensionDaysBucket(), tuple2._2()))
                .foreachRDD(rdd -> {
                    final JavaRDD<RegisterDaysBucketSpeedView> registerDaysBucketSpeedView = JavaSparkContext.fromSparkContext(rdd.context())
                            .parallelize(rdd.collect());
                    javaFunctions(registerDaysBucketSpeedView)
                            .writerBuilder("demo", "register_days_speed_view", mapToRow(RegisterDaysBucketSpeedView.class))
                            .saveToCassandra();
                });

        // 计算分时注册实时视图
        dimensionDateTimeStream
                .map(tuple2 -> RegisterHoursBucketSpeedView.valueOf(tuple2._1().toDimensionHoursBucket(), tuple2._2()))
                .foreachRDD(rdd -> {
                    final JavaRDD<RegisterHoursBucketSpeedView> registerDaysBucketSpeedView = JavaSparkContext.fromSparkContext(rdd.context())
                            .parallelize(rdd.collect());
                    javaFunctions(registerDaysBucketSpeedView)
                            .writerBuilder("demo", "register_hours_speed_view", mapToRow(RegisterHoursBucketSpeedView.class))
                            .saveToCassandra();
                });


        streamingContext.start();
        streamingContext.awaitTermination();
    }

}
