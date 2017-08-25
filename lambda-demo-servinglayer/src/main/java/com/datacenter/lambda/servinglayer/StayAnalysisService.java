package com.datacenter.lambda.servinglayer;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;

import com.datacenter.lambda.common.Dimension;
import com.datacenter.lambda.common.DimensionEnum;
import com.datacenter.lambda.common.DimensionEnumCodec;
import com.datacenter.lambda.common.TimestampBucketUtil;
import com.datacenter.lambda.servinglayer.model.AnalysisResult;
import com.datacenter.lambda.servinglayer.model.StayDto;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;


/**
 * 留存分析
 * Created by zuoc on 2017/7/25.
 */
@Service
public class StayAnalysisService {

    @Autowired
    private CassandraTemplate cassandraTemplate;

    /**
     * 留存天数
     */
    private List<Integer> stayDays = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 14, 30);

    /**
     * 用户留存分析
     *
     * @param dimension 统计样本数据维度
     * @param left      统计样本数据时间区间
     * @param right     统计样本数据时间区间
     */
    public AnalysisResult analysis(Dimension dimension, LocalDate left, LocalDate right, Object... parameters) {
        final Map<Integer, Set<Long>> daysBucketRegisterIds = getDaysBucketRegisterIds(dimension, left, right);
        final Map<Integer, Set<Long>> daysBucketLoginIds = getDaysBucketLoginIds(dimension, left, right);

        final LocalDate now = LocalDate.now();
        final List<StayDto> stayDtos = daysBucketRegisterIds.entrySet().parallelStream()
                .map(entry -> {
                    final int registerDaysBucket = entry.getKey();
                    final Set<Long> registerIds = entry.getValue();
                    if (registerDaysBucket >= now.toEpochDay()) {
                        return null;
                    }
                    final List<StayDto.StayItem> stayItems = new ArrayList<>();
                    for (final int stayDay : stayDays) {
                        final int loginDaysBucket = registerDaysBucket + stayDay;
                        if (loginDaysBucket >= now.toEpochDay()) {
                            break;
                        }
                        final Set<Long> loginIds = daysBucketLoginIds.get(loginDaysBucket);
                        if (loginIds != null) {
                            final Set<Long> intersectSet = Sets.intersection(registerIds, loginIds);
                            stayItems.add(StayDto.valueOf(stayDay, intersectSet.size(), registerIds.size()));
                        } else {
                            stayItems.add(StayDto.valueOf(stayDay, 0, registerIds.size()));
                        }
                    }
                    return StayDto.valueOf(TimestampBucketUtil.ofDayBucket(registerDaysBucket), registerIds.size(), stayItems);
                })
                .filter(x -> x != null)
                .collect(toList());

        return AnalysisResult.offline(stayDtos);
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
    private Map<Integer, Set<Long>> getDaysBucketLoginIds(Dimension dimension, LocalDate left, LocalDate right) {
        final Select selectLogin = QueryBuilder.select("days_bucket", "player_id").from("login_days_player_batch_view");

        final int maxStayDay = Collections.max(stayDays);
        selectLogin.where(QueryBuilder.eq("dimension", dimension.getDimensionType()))
                .and(QueryBuilder.eq("dimension_value", dimension.getDimensionValue()))
                .and(QueryBuilder.gte("days_bucket", left.toEpochDay()))
                .and(QueryBuilder.lte("days_bucket", right.plusDays(maxStayDay).toEpochDay()));

        return cassandraTemplate.query(selectLogin, ((row, rowNum) -> ImmutablePair.of(row.getInt("days_bucket"), row.getLong("player_id"))))
                .parallelStream()
                .collect(Collectors.groupingBy(
                        pair -> pair.getLeft(),
                        Collectors.mapping(ImmutablePair::getRight, toSet())
                ));
    }

    public static void main(String[] args) {
        Cluster cluster = Cluster.builder().addContactPoints("localhost").build();
        cluster.getConfiguration().getCodecRegistry().register(new DimensionEnumCodec());
        Session session = cluster.connect("demo");

        CassandraTemplate template = new CassandraTemplate(session);

        StayAnalysisService stayAnalysisService = new StayAnalysisService();
        stayAnalysisService.cassandraTemplate = template;

        Stopwatch stopwatch = Stopwatch.createStarted();

        final LocalDate left = LocalDate.of(2016, 7, 20);
        final LocalDate right = LocalDate.of(2017, 9, 27);

        System.out.println(stayAnalysisService.analysis(Dimension.valueOf(DimensionEnum.ALL, null), left, right));

        System.out.println(stopwatch.stop());

        session.close();
        cluster.close();

    }

}
