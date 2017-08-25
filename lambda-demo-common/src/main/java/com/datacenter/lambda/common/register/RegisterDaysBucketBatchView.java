package com.datacenter.lambda.common.register;

import com.datacenter.lambda.common.DimensionDaysBucket;
import com.datacenter.lambda.common.DimensionEnum;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;

/**
 * Created by zuoc on 2017/7/24.
 */
@Table(keyspace = "demo", name = "register_days_batch_view")
public class RegisterDaysBucketBatchView implements Serializable {

    @PartitionKey
    private DimensionEnum dimension;

    @PartitionKey(1)
    @Column(name = "dimension_value")
    private String dimensionValue;

    @ClusteringColumn
    @Column(name = "days_bucket")
    private int daysBucket;

    private long amount;

    public static RegisterDaysBucketBatchView valueOf(DimensionDaysBucket dimensionDaysBucket, int registerAmount) {
        final RegisterDaysBucketBatchView view = new RegisterDaysBucketBatchView();
        view.dimension = dimensionDaysBucket.getDimension().getDimensionType();
        view.dimensionValue = dimensionDaysBucket.getDimension().getDimensionValue();
        view.daysBucket = dimensionDaysBucket.getDaysBucket();
        view.amount = registerAmount;
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
