package com.datacenter.lambda.common;

import java.io.Serializable;
import java.util.Date;

/**
 * 维度统计时间桶(天)
 * Created by zuoc on 2017/7/24.
 */
public class DimensionDaysBucket implements Serializable {

    private Dimension dimension;

    private int daysBucket;

    public static DimensionDaysBucket valueOf(final Dimension dimension, Date date) {
        final DimensionDaysBucket bucket = new DimensionDaysBucket();
        bucket.dimension = dimension;
        bucket.daysBucket = TimestampBucketUtil.toDayBucket(date);
        return bucket;
    }

    public Dimension getDimension() {
        return dimension;
    }

    public void setDimension(Dimension dimension) {
        this.dimension = dimension;
    }

    public int getDaysBucket() {
        return daysBucket;
    }

    public void setDaysBucket(int daysBucket) {
        this.daysBucket = daysBucket;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DimensionDaysBucket that = (DimensionDaysBucket) o;

        if (daysBucket != that.daysBucket) return false;
        return dimension.equals(that.dimension);

    }

    @Override
    public int hashCode() {
        int result = dimension.hashCode();
        result = 31 * result + (int) (daysBucket ^ (daysBucket >>> 32));
        return result;
    }
}
