package com.datacenter.lambda.common.login;

import com.datacenter.lambda.common.DimensionDaysBucket;
import com.datacenter.lambda.common.DimensionEnum;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;

/**
 * Created by zuoc on 2017/7/25.
 */
@Table(keyspace = "demo", name = "login_days_speed_view")
public class LoginDaysBucketSpeedView implements Serializable {

    @PartitionKey(1)
    private DimensionEnum dimension;

    @PartitionKey(2)
    @Column(name = "dimension_value")
    private String dimensionValue;

    @ClusteringColumn(1)
    @Column(name = "days_bucket")
    private int daysBucket;

    private long amount;

    public static LoginDaysBucketSpeedView valueOf(DimensionDaysBucket dimensionDaysBucket, int loginAmount) {
        final LoginDaysBucketSpeedView view = new LoginDaysBucketSpeedView();
        view.dimension = dimensionDaysBucket.getDimension().getDimensionType();
        view.dimensionValue = dimensionDaysBucket.getDimension().getDimensionValue();
        view.daysBucket = dimensionDaysBucket.getDaysBucket();
        view.amount = loginAmount;
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

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }
}
