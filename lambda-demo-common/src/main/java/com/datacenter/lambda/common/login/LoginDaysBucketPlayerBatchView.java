package com.datacenter.lambda.common.login;

import com.datacenter.lambda.common.DimensionDaysBucket;
import com.datacenter.lambda.common.DimensionEnum;
import com.datacenter.lambda.common.DimensionEnumCodec;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;

/**
 * Created by zuoc on 2017/7/24.
 */
@Table(keyspace = "demo", name = "login_days_player_batch_view")
public class LoginDaysBucketPlayerBatchView implements Serializable {

    @PartitionKey
    @Column(codec = DimensionEnumCodec.class)
    private DimensionEnum dimension;

    @PartitionKey(1)
    @Column(name = "dimension_value")
    private String dimensionValue;

    @ClusteringColumn
    @Column(name = "days_bucket")
    private int daysBucket;

    @ClusteringColumn(1)
    @Column(name = "player_id")
    private long playerId;

    public static LoginDaysBucketPlayerBatchView valueOf(DimensionDaysBucket dimensionDaysBucket, long playerId) {
        final LoginDaysBucketPlayerBatchView view = new LoginDaysBucketPlayerBatchView();
        view.dimension = dimensionDaysBucket.getDimension().getDimensionType();
        view.dimensionValue = dimensionDaysBucket.getDimension().getDimensionValue();
        view.daysBucket = dimensionDaysBucket.getDaysBucket();
        view.playerId = playerId;
        return view;
    }

    public DimensionEnum getDimension() {
        return dimension;
    }

    public void setDimension(DimensionEnum dimension) {
        this.dimension = dimension;
    }

    public String getDimensionValue() {
        return dimensionValue;
    }

    public void setDimensionValue(String dimensionValue) {
        this.dimensionValue = dimensionValue;
    }

    public int getDaysBucket() {
        return daysBucket;
    }

    public void setDaysBucket(int daysBucket) {
        this.daysBucket = daysBucket;
    }

    public long getPlayerId() {
        return playerId;
    }

    public void setPlayerId(long playerId) {
        this.playerId = playerId;
    }

    @Override
    public String toString() {
        return "LoginDaysBucketPlayerBatchView{" +
                "dimension=" + dimension +
                ", dimensionValue='" + dimensionValue + '\'' +
                ", daysBucket=" + daysBucket +
                ", playerId=" + playerId +
                '}';
    }
}
