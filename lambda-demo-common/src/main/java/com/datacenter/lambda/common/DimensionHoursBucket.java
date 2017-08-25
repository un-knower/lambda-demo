package com.datacenter.lambda.common;

import java.io.Serializable;
import java.util.Date;

/**
 * 维度统计时间桶(小时)
 * Created by zuoc on 2017/7/24.
 */
public class DimensionHoursBucket implements Serializable {

    private Dimension dimension;

    private int hoursBucket;

    public static DimensionHoursBucket valueOf(final Dimension dimension, Date date) {
        final DimensionHoursBucket bucket = new DimensionHoursBucket();
        bucket.dimension = dimension;
        bucket.hoursBucket = TimestampBucketUtil.toHourBucket(date);
        return bucket;
    }

    public Dimension getDimension() {
        return dimension;
    }

    public void setDimension(Dimension dimension) {
        this.dimension = dimension;
    }

    public int getHoursBucket() {
        return hoursBucket;
    }

    public void setHoursBucket(int hoursBucket) {
        this.hoursBucket = hoursBucket;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DimensionHoursBucket that = (DimensionHoursBucket) o;

        if (hoursBucket != that.hoursBucket) return false;
        return dimension.equals(that.dimension);

    }

    @Override
    public int hashCode() {
        int result = dimension.hashCode();
        result = 31 * result + (int) (hoursBucket ^ (hoursBucket >>> 32));
        return result;
    }
}
