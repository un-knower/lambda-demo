package com.datacenter.lambda.common;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by zuoc on 2017/7/25.
 */
public class DimensionDateTime implements Serializable {

    private Dimension dimension;

    private Date date;

    public static DimensionDateTime valueOf(final Dimension dimension, Date date) {
        final DimensionDateTime dimensionDateTime = new DimensionDateTime();
        dimensionDateTime.dimension = dimension;
        dimensionDateTime.date = date;
        return dimensionDateTime;
    }

    public DimensionDaysBucket toDimensionDaysBucket() {
        return DimensionDaysBucket.valueOf(dimension, date);
    }

    public DimensionHoursBucket toDimensionHoursBucket() {
        return DimensionHoursBucket.valueOf(dimension, date);
    }

    public Dimension getDimension() {
        return dimension;
    }

    public void setDimension(Dimension dimension) {
        this.dimension = dimension;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DimensionDateTime that = (DimensionDateTime) o;

        if (!dimension.equals(that.dimension)) return false;
        return date.equals(that.date);

    }

    @Override
    public int hashCode() {
        int result = dimension.hashCode();
        result = 31 * result + date.hashCode();
        return result;
    }
}
