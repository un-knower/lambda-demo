package com.datacenter.lambda.servinglayer;

import com.google.common.base.Stopwatch;

import com.datacenter.lambda.common.Dimension;
import com.datacenter.lambda.common.DimensionEnum;
import com.datacenter.lambda.common.DimensionEnumCodec;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toSet;

/**
 * 流失分析
 * Created by zuoc on 2017/7/25.
 */
@Service
public class LostAnalysisService {

    /**
     * 流失天数
     */
    private List<Integer> lostDays = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 14, 30);

    @Autowired
    private CassandraTemplate cassandraTemplate;

    public AnalysisResult analysis(Dimension dimension, LocalDate left, LocalDate right, Object... parameters) {
        final Map<Integer, Set<Long>> daysBucketRegisterIds = getDaysBucketRegisterIds(dimension, left, right);
        final Map<Long, Set<Integer>> loginIdDaysBuckets = getLoginIdDaysBuckets(dimension, left, right);

        final LocalDate now = LocalDate.now();
        daysBucketRegisterIds.entrySet().parallelStream()
                .map(entry -> {
                    final int registerDaysBucket = entry.getKey();
                    final Set<Long> registerIds = entry.getValue();
                    if (registerDaysBucket >= now.toEpochDay()) {
                        return null;
                    }
                    registerIds.stream().forEach(registerId -> {
                        // 已排序的登陆时间桶
                        final Integer[] loginDayBuckets = loginIdDaysBuckets.get(registerId).toArray(new Integer[]{});
                        if (loginDayBuckets == null || loginDayBuckets.length <= 0) {
                            // 注册用户在样本时间内未登陆过,视为流失。
                        } else {
                            final AtomicInteger cursor = new AtomicInteger();
                            for (final int lostDay : lostDays) {
                                final int lostDayBucket = registerDaysBucket + lostDay;
                                if (loginDayBuckets[cursor.getAndIncrement()] > lostDayBucket) {
                                    // TODO 流失计数

                                } else {

                                }
                            }
                        }
                    });

                    //  以下注释掉
                    return null;
                });
        return null;
    }

    /**
     * 获取注册样本数据
     */
    private Map<Integer, Set<Long>> getDaysBucketRegisterIds(Dimension dimension, LocalDate left, LocalDate right) {
        final Select selectRegister = QueryBuilder.select("days_bucket", "player_id").from("register_days_player_batch_view");

        selectRegister.where(QueryBuilder.eq("dimension", dimension.getDimensionType()))
                .and(QueryBuilder.eq("dimension_value", dimension.getDimensionValue()))
                .and(QueryBuilder.gte("days_bucket", left.toEpochDay()))
                .and(QueryBuilder.lte("days_bucket", right.toEpochDay()));

        return cassandraTemplate.query(selectRegister, ((row, rowNum) -> ImmutablePair.of(row.getInt("days_bucket"), row.getLong("player_id"))))
                .parallelStream()
                .collect(Collectors.groupingBy(
                        pair -> pair.getLeft(),
                        Collectors.mapping(ImmutablePair::getRight, toSet())
                ));
    }

    /**
     * 获取登陆样本数据
     */
    private Map<Long, Set<Integer>> getLoginIdDaysBuckets(Dimension dimension, LocalDate left, LocalDate right) {
        final Select selectLogin = QueryBuilder.select("days_bucket", "player_id").from("login_days_player_batch_view");

        final int maxLostDay = Collections.max(lostDays);
        selectLogin.where(QueryBuilder.eq("dimension", dimension.getDimensionType()))
                .and(QueryBuilder.eq("dimension_value", dimension.getDimensionValue()))
                .and(QueryBuilder.gte("days_bucket", left.toEpochDay()))
                .and(QueryBuilder.lte("days_bucket", right.plusDays(maxLostDay).toEpochDay()));

        return cassandraTemplate.query(selectLogin, ((row, rowNum) -> ImmutablePair.of(row.getLong("player_id"), row.getInt("days_bucket"))))
                .parallelStream()
                .collect(Collectors.groupingBy(
                        pair -> pair.getLeft(),
                        Collectors.mapping(ImmutablePair::getRight, toCollection(TreeSet::new))
                ));
    }

    public static void main(String[] args) {
        Cluster cluster = Cluster.builder().addContactPoints("localhost").build();
        cluster.getConfiguration().getCodecRegistry().register(new DimensionEnumCodec());
        Session session = cluster.connect("demo");

        CassandraTemplate template = new CassandraTemplate(session);

        LostAnalysisService lostAnalysisService = new LostAnalysisService();
        lostAnalysisService.cassandraTemplate = template;

        Stopwatch stopwatch = Stopwatch.createStarted();

        final LocalDate left = LocalDate.of(2016, 7, 20);
        final LocalDate right = LocalDate.of(2017, 9, 27);

        System.out.println(lostAnalysisService.analysis(Dimension.valueOf(DimensionEnum.ALL, null), left, right));

        System.out.println(stopwatch.stop());

        session.close();
        cluster.close();
    }
}
