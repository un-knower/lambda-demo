package com.datacenter.lambda.servinglayer;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Range;

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
 * 注册分析
 * Created by zuoc on 2017/7/25.
 */
@Service
public class RegisterAnalysisService {

    @Autowired
    private CassandraTemplate cassandraTemplate;

    public AnalysisResult daysAnalysis(Dimension dimension, LocalDate left, LocalDate right) {
        final LocalDate now = LocalDate.now();
        final Range<LocalDate> range = fixtime(left, right, now);
        final Map<Integer, Long> offline = getDaysBucketBatchRegisterIds(dimension, range.lowerEndpoint(), range.upperEndpoint());
        final Map<Integer, Long> realtime = getDaysBucketSpeedRegisterIds(dimension, now, now);
        return AnalysisResult.combine(offline, realtime);
    }

    public AnalysisResult hoursAnalysis(Dimension dimension, LocalDate left, LocalDate right) {
        final LocalDate now = LocalDate.now();
        final Range<LocalDate> range = fixtime(left, right, now);
        final Map<Integer, Long> offline = getHoursBucketBatchRegisterIds(dimension, range.lowerEndpoint(), range.upperEndpoint());
        final Map<Integer, Long> realtime = getHoursBucketSpeedRegisterIds(dimension, now, now);
        return AnalysisResult.combine(offline, realtime);
    }

    private Map<Integer, Long> getDaysBucketBatchRegisterIds(Dimension dimension, LocalDate left, LocalDate right) {
        final Select selectLogin = QueryBuilder
                .select("days_bucket", "amount")
                .from("register_days_batch_view");

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

    private Map<Integer, Long> getDaysBucketSpeedRegisterIds(Dimension dimension, LocalDate left, LocalDate right) {
        final Select selectLogin = QueryBuilder
                .select("days_bucket", "amount")
                .from("register_days_speed_view");

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


    private Map<Integer, Long> getHoursBucketBatchRegisterIds(Dimension dimension, LocalDate left, LocalDate right) {
        final Select selectLogin = QueryBuilder
                .select("hours_bucket", "amount")
                .from("register_hours_batch_view");


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

    private Map<Integer, Long> getHoursBucketSpeedRegisterIds(Dimension dimension, LocalDate left, LocalDate right) {
        final Select selectLogin = QueryBuilder
                .select("hours_bucket", "amount")
                .from("register_hours_speed_view");


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

    private Range<LocalDate> fixtime(LocalDate left, LocalDate right, LocalDate now) {
        if (left.isAfter(right)) {
            throw new IllegalArgumentException("样本数据时间区间异常");
        }
        if (right.isAfter(now)) {
            throw new IllegalArgumentException("样本数据时间区间异常");
        }
        if (right.equals(now)) {
            right = now.minusDays(1);
        }
        return Range.closed(left, right);
    }

    public static void main(String[] args) {
        Cluster cluster = Cluster.builder().addContactPoints("localhost").build();
        cluster.getConfiguration().getCodecRegistry().register(new DimensionEnumCodec());
        Session session = cluster.connect("demo");

        CassandraTemplate template = new CassandraTemplate(session);

        RegisterAnalysisService registerAnalysisService = new RegisterAnalysisService();
        registerAnalysisService.cassandraTemplate = template;

        Stopwatch stopwatch = Stopwatch.createStarted();

        final LocalDate left = LocalDate.of(2017, 7, 25);
        final LocalDate right = LocalDate.of(2017, 7, 26);


        System.out.println(registerAnalysisService.daysAnalysis(Dimension.valueOf(DimensionEnum.ALL, null), left, right));

        System.out.println(stopwatch.stop());

        session.close();
        cluster.close();


    }
}
