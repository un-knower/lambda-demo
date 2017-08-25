package com.datacenter.lambda.common.register;

import com.datacenter.lambda.common.DimensionEnum;
import com.datacenter.lambda.common.DimensionHoursBucket;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;

/**
 * Created by zuoc on 2017/7/24.
 */
@Table(keyspace = "demo", name = "register_hours_batch_view")
public class RegisterHoursBucketBatchView implements Serializable {

    @PartitionKey
    private DimensionEnum dimension;

    @PartitionKey(1)
    @Column(name = "dimension_value")
    private String dimensionValue;

    @ClusteringColumn
    @Column(name = "hours_bucket")
    private int hoursBucket;

    private long amount;

    public static RegisterHoursBucketBatchView valueOf(DimensionHoursBucket dimensionHoursBucket, int registerAmount) {
        final RegisterHoursBucketBatchView view = new RegisterHoursBucketBatchView();
        view.dimension = dimensionHoursBucket.getDimension().getDimensionType();
        view.dimensionValue = dimensionHoursBucket.getDimension().getDimensionValue();
        view.hoursBucket = dimensionHoursBucket.getHoursBucket();
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

    public int getHoursBucket() {
        return hoursBucket;
    }

    public void setHoursBucket(int hoursBucket) {
        this.hoursBucket = hoursBucket;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }
}
