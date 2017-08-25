package com.datacenter.lambda.batchlayer;

import com.datacenter.lambda.common.DimensionDaysBucket;
import com.datacenter.lambda.common.DimensionHoursBucket;
import com.datacenter.lambda.common.login.LoginDaysBucketBatchView;
import com.datacenter.lambda.common.login.LoginDaysBucketPlayerBatchView;
import com.datacenter.lambda.common.login.LoginHoursBucketBatchView;
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
 * Created by zuoc on 2017/7/24.
 */
public class LoginBatch {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("LoginBatch")
                .config("spark.mongodb.input.uri", "mongodb://docker.moetouch.com:27017/gs2._")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());


        final CassandraConnector cassandraConnector = CassandraConnector.apply(jsc.sc());
        try (final Session session = cassandraConnector.openSession()) {
            session.execute("USE demo");
            session.execute("CREATE TABLE IF NOT EXISTS login_days_batch_view (" +
                    "    dimension varchar," +
                    "    dimension_value varchar," +
                    "    days_bucket int," +
                    "    amount bigint," +
                    "    PRIMARY KEY ((dimension, dimension_value), days_bucket)" +
                    ");");

            session.execute("CREATE TABLE IF NOT EXISTS login_hours_batch_view (" +
                    "    dimension varchar," +
                    "    dimension_value varchar," +
                    "    hours_bucket int," +
                    "    amount bigint," +
                    "    PRIMARY KEY ((dimension, dimension_value), hours_bucket)" +
                    ");");

            session.execute("CREATE TABLE IF NOT EXISTS login_days_player_batch_view (" +
                    "    dimension varchar," +
                    "    dimension_value varchar," +
                    "    days_bucket int," +
                    "    player_id bigint," +
                    "    PRIMARY KEY ((dimension, dimension_value), days_bucket, player_id)" +
                    ");");
        }

        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", "originalLoginSummary");
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);


        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(jsc, readConfig);

        JavaRDD<Document> filterRDD = mongoRDD.filter(document -> document.get("players", List.class).size() > 0).cache();

        JavaRDD<LoginDaysBucketBatchView> loginDaysBucketBatchView = createLoginDaysBucketBatchView(filterRDD);
        javaFunctions(loginDaysBucketBatchView)
                .writerBuilder("demo", "login_days_batch_view", mapToRow(LoginDaysBucketBatchView.class))
                .saveToCassandra();

        JavaRDD<LoginDaysBucketPlayerBatchView> loginDaysBucketPlayerBatchView = createLoginDaysBucketPlayerBatchView(filterRDD);
        javaFunctions(loginDaysBucketPlayerBatchView)
                .writerBuilder("demo", "login_days_player_batch_view", mapToRow(LoginDaysBucketPlayerBatchView.class))
                .saveToCassandra();

        JavaRDD<LoginHoursBucketBatchView> loginHoursBucketBatchView = createLoginHoursBucketBatchView(filterRDD);
        javaFunctions(loginHoursBucketBatchView)
                .writerBuilder("demo", "login_hours_batch_view", mapToRow(LoginHoursBucketBatchView.class))
                .saveToCassandra();

    }

    private static JavaRDD<LoginDaysBucketBatchView> createLoginDaysBucketBatchView(JavaRDD<Document> documentRDD) {
        return documentRDD
                .flatMapToPair(document -> {
                    final Integer loginCount = document.get("players", List.class).size();
                    return Arrays.asList(
                            new Tuple2<DimensionDaysBucket, Integer>(extractAllDimensionDaysBucket(document), loginCount),
                            new Tuple2<DimensionDaysBucket, Integer>(extractOperatorDimensionDaysBucket(document), loginCount),
                            new Tuple2<DimensionDaysBucket, Integer>(extractAppDimensionDaysBucket(document), loginCount),
                            new Tuple2<DimensionDaysBucket, Integer>(extractServerDimensionDaysBucket(document), loginCount)
                    ).iterator();
                })

                .reduceByKey(((v1, v2) -> v1 + v2))

                .map(tuple2 -> LoginDaysBucketBatchView.valueOf(tuple2._1(), tuple2._2()));
    }

    private static JavaRDD<LoginHoursBucketBatchView> createLoginHoursBucketBatchView(JavaRDD<Document> documentRDD) {
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

                .map(tuple2 -> LoginHoursBucketBatchView.valueOf(tuple2._1(), tuple2._2()));
    }

    private static JavaRDD<LoginDaysBucketPlayerBatchView> createLoginDaysBucketPlayerBatchView(JavaRDD<Document> documentRDD) {
        return documentRDD.flatMap(document -> {
            final List<LoginDaysBucketPlayerBatchView> result = new ArrayList<>();
            final List<Long> playerIds = document.get("players", List.class);
            for (final long playerId : playerIds) {
                result.add(LoginDaysBucketPlayerBatchView.valueOf(extractAllDimensionDaysBucket(document), playerId));
                result.add(LoginDaysBucketPlayerBatchView.valueOf(extractOperatorDimensionDaysBucket(document), playerId));
                result.add(LoginDaysBucketPlayerBatchView.valueOf(extractAppDimensionDaysBucket(document), playerId));
                result.add(LoginDaysBucketPlayerBatchView.valueOf(extractServerDimensionDaysBucket(document), playerId));
            }
            return result.iterator();
        });
    }

}
