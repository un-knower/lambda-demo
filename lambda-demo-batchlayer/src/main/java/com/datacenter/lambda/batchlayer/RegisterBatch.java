package com.datacenter.lambda.batchlayer;

import com.datacenter.lambda.common.DimensionDaysBucket;
import com.datacenter.lambda.common.DimensionHoursBucket;
import com.datacenter.lambda.common.register.RegisterDaysBucketBatchView;
import com.datacenter.lambda.common.register.RegisterDaysBucketPlayerBatchView;
import com.datacenter.lambda.common.register.RegisterHoursBucketBatchView;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import static com.datacenter.lambda.batchlayer.MongoDimensionExtracter.*;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

/**
 * Created by zuoc on 2017/7/14.
 */
public class RegisterBatch {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("RegisterBatch")
                .config("spark.mongodb.input.uri", "mongodb://docker.moetouch.com:27017/gs2._")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // 定义 cassandra 批视图
        final CassandraConnector cassandraConnector = CassandraConnector.apply(jsc.sc());
        try (final Session session = cassandraConnector.openSession()) {
            session.execute("USE demo");
            session.execute("CREATE TABLE IF NOT EXISTS register_days_batch_view (" +
                    "    dimension varchar," +
                    "    dimension_value varchar," +
                    "    days_bucket int," +
                    "    amount bigint," +
                    "    PRIMARY KEY ((dimension, dimension_value), days_bucket)" +
                    ");");

            session.execute("CREATE TABLE IF NOT EXISTS register_days_player_batch_view (" +
                    "    dimension varchar," +
                    "    dimension_value varchar," +
                    "    days_bucket int," +
                    "    player_id bigint," +
                    "    PRIMARY KEY ((dimension, dimension_value), days_bucket, player_id)" +
                    ");");

            session.execute("CREATE TABLE IF NOT EXISTS register_hours_batch_view (" +
                    "    dimension varchar," +
                    "    dimension_value varchar," +
                    "    hours_bucket int," +
                    "    amount bigint," +
                    "    PRIMARY KEY ((dimension, dimension_value), hours_bucket)" +
                    ");");
        }

        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", "originalRegisterSummary");
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        // 加载 mongodb 文档
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(jsc, readConfig);

        // 过滤空文档
        JavaRDD<Document> filterRDD = mongoRDD.filter(document -> document.get("accounts", List.class).size() > 0).cache();

        // 计算每日注册批视图
        JavaRDD<RegisterDaysBucketBatchView> registerDaysBucketBatchView = createRegisterDaysBucketBatchView(filterRDD);
        javaFunctions(registerDaysBucketBatchView)
                .writerBuilder("demo", "register_days_batch_view", mapToRow(RegisterDaysBucketBatchView.class))
                .saveToCassandra();

        // 计算每日注册用户批视图
        JavaRDD<RegisterDaysBucketPlayerBatchView> registerDaysBucketPlayerBatchView = createRegisterDaysBucketPlayerBatchView(filterRDD);
        javaFunctions(registerDaysBucketPlayerBatchView)
                .writerBuilder("demo", "register_days_player_batch_view", mapToRow(RegisterDaysBucketPlayerBatchView.class))
                .saveToCassandra();


        // 计算分时注册批视图
        JavaRDD<RegisterHoursBucketBatchView> registerHourssBucketBatchView = createRegisterHoursBucketBatchView(filterRDD);
        javaFunctions(registerHourssBucketBatchView)
                .writerBuilder("demo", "register_hours_batch_view", mapToRow(RegisterHoursBucketBatchView.class))
                .saveToCassandra();

        jsc.close();

    }

    private static JavaRDD<RegisterDaysBucketBatchView> createRegisterDaysBucketBatchView(JavaRDD<Document> documentRDD) {
        return documentRDD
                .flatMapToPair(document -> {
                    final int registerAmount = document.get("players", List.class).size();
                    return Arrays.asList(
                            new Tuple2<DimensionDaysBucket, Integer>(extractAllDimensionDaysBucket(document), registerAmount),
                            new Tuple2<DimensionDaysBucket, Integer>(extractServerDimensionDaysBucket(document), registerAmount),
                            new Tuple2<DimensionDaysBucket, Integer>(extractOperatorDimensionDaysBucket(document), registerAmount),
                            new Tuple2<DimensionDaysBucket, Integer>(extractAppDimensionDaysBucket(document), registerAmount)
                    ).iterator();
                })
                .reduceByKey((v1, v2) -> v1 + v2)

                .map(tuple2 -> RegisterDaysBucketBatchView.valueOf(tuple2._1(), tuple2._2()));
    }

    private static JavaRDD<RegisterHoursBucketBatchView> createRegisterHoursBucketBatchView(JavaRDD<Document> documentRDD) {
        return documentRDD
                .flatMapToPair(document -> {
                    final int registerAmount = document.get("players", List.class).size();
                    return Arrays.asList(
                            new Tuple2<DimensionHoursBucket, Integer>(extractAllDimensionHoursBucket(document), registerAmount),
                            new Tuple2<DimensionHoursBucket, Integer>(extractServerDimensionHoursBucket(document), registerAmount),
                            new Tuple2<DimensionHoursBucket, Integer>(extractOperatorDimensionHoursBucket(document), registerAmount),
                            new Tuple2<DimensionHoursBucket, Integer>(extractAppDimensionHoursBucket(document), registerAmount)
                    ).iterator();
                })

                .reduceByKey((v1, v2) -> v1 + v2)

                .map(tuple2 -> RegisterHoursBucketBatchView.valueOf(tuple2._1(), tuple2._2()));
    }

    private static JavaRDD<RegisterDaysBucketPlayerBatchView> createRegisterDaysBucketPlayerBatchView(JavaRDD<Document> documentRDD) {
        return documentRDD.flatMap(document -> {
            final List<RegisterDaysBucketPlayerBatchView> views = new ArrayList<>();
            final List<Long> playerIds = document.get("players", List.class);
            for (final long playerId : playerIds) {
                views.add(RegisterDaysBucketPlayerBatchView.valueOf(extractAllDimensionDaysBucket(document), playerId));
                views.add(RegisterDaysBucketPlayerBatchView.valueOf(extractAppDimensionDaysBucket(document), playerId));
                views.add(RegisterDaysBucketPlayerBatchView.valueOf(extractOperatorDimensionDaysBucket(document), playerId));
                views.add(RegisterDaysBucketPlayerBatchView.valueOf(extractServerDimensionDaysBucket(document), playerId));
            }
            return views.iterator();
        });
    }


}
