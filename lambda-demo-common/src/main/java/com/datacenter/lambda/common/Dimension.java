package com.datacenter.lambda.common;

import java.io.Serializable;

/**
 * 统计维度
 * Created by zuoc on 2017/7/24.
 */
public class Dimension implements Serializable {

    private DimensionEnum dimensionType = DimensionEnum.ALL;

    private String dimensionValue = DimensionEnum.all();

    public static Dimension valueOf(DimensionEnum dimensionEnum, String dimensionValue) {
        final Dimension dimension = new Dimension();
        dimension.dimensionType = dimensionEnum;
        dimension.dimensionValue = dimensionEnum == DimensionEnum.ALL ? DimensionEnum.all() : dimensionValue;
        return dimension;
    }

    public DimensionEnum getDimensionType() {
        return dimensionType;
    }

    public void setDimensionType(DimensionEnum dimensionType) {
        this.dimensionType = dimensionType;
    }

    public String getDimensionValue() {
        return dimensionValue;
    }

    public void setDimensionValue(String dimensionValue) {
        this.dimensionValue = dimensionValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Dimension dimension = (Dimension) o;

        if (dimensionType != dimension.dimensionType) return false;
        return dimensionValue.equals(dimension.dimensionValue);

    }

    @Override
    public int hashCode() {
        int result = dimensionType.hashCode();
        result = 31 * result + dimensionValue.hashCode();
        return result;
    }
}
