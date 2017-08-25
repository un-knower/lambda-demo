package com.datacenter.lambda.servinglayer;

import com.google.common.base.Stopwatch;

import com.datacenter.lambda.common.Dimension;
import com.datacenter.lambda.common.DimensionEnum;
import com.datacenter.lambda.common.DimensionEnumCodec;
import com.datacenter.lambda.common.TimestampBucketUtil;
import com.datacenter.lambda.servinglayer.model.AnalysisResult;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * 活跃分析
 * Created by zuoc on 2017/7/25.
 */
@Service
public class LoginAnalysisService {

    @Autowired
    private CassandraTemplate cassandraTemplate;

    /**
     * 每日活跃
     *
     * @param dimension 数据样本维度
     * @param left      数据样本区间
     * @param right     数据样本区间
     */
    public AnalysisResult daysAnalysis(Dimension dimension, LocalDate left, LocalDate right) {
        return AnalysisResult.offline(getDaysBucketLoginIds(dimension, left, right));
    }

    /**
     * 每日分时活跃
     *
     * @param dimension 数据样本维度
     * @param left      数据样本区间
     * @param right     数据样本区间
     */
    public AnalysisResult hoursAnalysis(Dimension dimension, LocalDate left, LocalDate right) {
        return AnalysisResult.offline(getHoursBucketLoginIds(dimension, left, right));
    }

    private Map<Integer, Long> getHoursBucketLoginIds(Dimension dimension, LocalDate left, LocalDate right) {
        final Select selectLogin = QueryBuilder
                .select("hours_bucket", "amount")
                .from("login_hours_batch_view")
                .groupBy("hours_bucket");


        selectLogin.where(QueryBuilder.eq("dimension", dimension.getDimensionType()))
                .and(QueryBuilder.eq("dimension_value", dimension.getDimensionValue()))
                .and(QueryBuilder.gte("hours_bucket", TimestampBucketUtil.toHourBucket(left.atTime(LocalTime.MIN))))
                .and(QueryBuilder.lte("hours_bucket", TimestampBucketUtil.toHourBucket(right.atTime(LocalTime.MAX))));

        return cassandraTemplate.query(selectLogin, ((row, rowNum) -> ImmutablePair.of(row.getInt("hours_bucket"), row.getLong("amount"))))
                .parallelStream()
                .collect(Collectors.toMap(ImmutablePair::getLeft, ImmutablePair::getRight, (u, v) -> {
                    throw new IllegalStateException(String.format("Duplicate key %s", u));
                }, LinkedHashMap::new));
    }

    /**
     * 获取每日登陆样本数据
     */
    private Map<Integer, Long> getDaysBucketLoginIds(Dimension dimension, LocalDate left, LocalDate right) {
        final Select selectLogin = QueryBuilder
                .select("days_bucket", "amount")
                .from("login_days_batch_view")
                .groupBy("days_bucket");

        selectLogin.where(QueryBuilder.eq("dimension", dimension.getDimensionType()))
                .and(QueryBuilder.eq("dimension_value", dimension.getDimensionValue()))
                .and(QueryBuilder.gte("days_bucket", left.toEpochDay()))
                .and(QueryBuilder.lte("days_bucket", right.toEpochDay()));

        return cassandraTemplate.query(selectLogin, ((row, rowNum) -> ImmutablePair.of(row.getInt("days_bucket"), row.getLong("amount"))))
                .parallelStream()
                .collect(Collectors.toMap(ImmutablePair::getLeft, ImmutablePair::getRight, (u, v) -> {
                    throw new IllegalStateException(String.format("Duplicate key %s", u));
                }, LinkedHashMap::new));
    }

    public static void main(String[] args) {
        Cluster cluster = Cluster.builder().addContactPoints("localhost").build();
        cluster.getConfiguration().getCodecRegistry().register(new DimensionEnumCodec());
        Session session = cluster.connect("demo");

        CassandraTemplate template = new CassandraTemplate(session);

        LoginAnalysisService loginAnalysisService = new LoginAnalysisService();
        loginAnalysisService.cassandraTemplate = template;

        Stopwatch stopwatch = Stopwatch.createStarted();

        final LocalDate left = LocalDate.of(2017, 6, 3);
        final LocalDate right = LocalDate.of(2017, 6, 3);

        System.out.println(loginAnalysisService.hoursAnalysis(Dimension.valueOf(DimensionEnum.ALL, null), left, right));

        System.out.println(stopwatch.stop());

        session.close();
        cluster.close();


    }
}
