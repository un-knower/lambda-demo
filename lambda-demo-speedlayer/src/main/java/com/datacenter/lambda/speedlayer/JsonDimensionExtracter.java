package com.datacenter.lambda.speedlayer;

import com.alibaba.fastjson.JSONObject;
import com.datacenter.lambda.common.Dimension;
import com.datacenter.lambda.common.DimensionDateTime;

import java.util.Date;

import static com.datacenter.lambda.common.DimensionEnum.*;


/**
 * Created by zuoc on 2017/7/25.
 */
public class JsonDimensionExtracter {

    private static final String DATE_TIME_KEY = "time";

    public static DimensionDateTime extractAllDimensionDateTime(final JSONObject data) {
        final Dimension dimension = Dimension.valueOf(ALL, all());
        return DimensionDateTime.valueOf(dimension, getDateTime(data));
    }

    public static DimensionDateTime extractAppDimensionDateTime(final JSONObject data) {
        final Dimension dimension = Dimension.valueOf(APP, data.getString("aid") == null ? "null" : data.getString("aid"));
        return DimensionDateTime.valueOf(dimension, getDateTime(data));
    }

    public static DimensionDateTime extractServerDimensionDateTime(final JSONObject data) {
        final Dimension dimension = Dimension.valueOf(SERVER, String.valueOf(data.getInteger("sid")));
        return DimensionDateTime.valueOf(dimension, getDateTime(data));
    }

    public static DimensionDateTime extractOperatorDimensionDateTime(final JSONObject data) {
        final Dimension dimension = Dimension.valueOf(OPERATOR, String.valueOf(data.getInteger("oid")));
        return DimensionDateTime.valueOf(dimension, getDateTime(data));
    }


    private static Date getDateTime(JSONObject data) {
        return data.getDate(DATE_TIME_KEY);
    }

}
