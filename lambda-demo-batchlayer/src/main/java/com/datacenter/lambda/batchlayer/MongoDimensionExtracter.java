package com.datacenter.lambda.batchlayer;


import com.datacenter.lambda.common.Dimension;
import com.datacenter.lambda.common.DimensionDaysBucket;
import com.datacenter.lambda.common.DimensionHoursBucket;

import org.bson.Document;

import java.util.Date;

import static com.datacenter.lambda.common.DimensionEnum.*;

/**
 * Created by zuoc on 2017/7/24.
 */
public class MongoDimensionExtracter {

    private static final String DATE_TIME_KEY = "time";

    public static DimensionDaysBucket extractAllDimensionDaysBucket(final Document document) {
        final Dimension dimension = Dimension.valueOf(ALL, all());
        return DimensionDaysBucket.valueOf(dimension, getDateTime(document));
    }

    public static DimensionDaysBucket extractAppDimensionDaysBucket(final Document document) {
        final Dimension dimension = Dimension.valueOf(APP, document.getString("aid") == null ? "null" : document.getString("aid"));
        return DimensionDaysBucket.valueOf(dimension, getDateTime(document));
    }

    public static DimensionDaysBucket extractServerDimensionDaysBucket(final Document document) {
        final Dimension dimension = Dimension.valueOf(SERVER, String.valueOf(document.getInteger("sid")));
        return DimensionDaysBucket.valueOf(dimension, getDateTime(document));
    }

    public static DimensionDaysBucket extractOperatorDimensionDaysBucket(final Document document) {
        final Dimension dimension = Dimension.valueOf(OPERATOR, String.valueOf(document.getInteger("oid")));
        return DimensionDaysBucket.valueOf(dimension, getDateTime(document));
    }

    public static DimensionHoursBucket extractAllDimensionHoursBucket(final Document document) {
        final Dimension dimension = Dimension.valueOf(ALL, all());
        return DimensionHoursBucket.valueOf(dimension, getDateTime(document));
    }

    public static DimensionHoursBucket extractAppDimensionHoursBucket(final Document document) {
        final Dimension dimension = Dimension.valueOf(APP, document.getString("aid") == null ? "null" : document.getString("aid"));
        return DimensionHoursBucket.valueOf(dimension, getDateTime(document));
    }

    public static DimensionHoursBucket extractServerDimensionHoursBucket(final Document document) {
        final Dimension dimension = Dimension.valueOf(SERVER, String.valueOf(document.getInteger("sid")));
        return DimensionHoursBucket.valueOf(dimension, getDateTime(document));
    }

    public static DimensionHoursBucket extractOperatorDimensionHoursBucket(final Document document) {
        final Dimension dimension = Dimension.valueOf(OPERATOR, String.valueOf(document.getInteger("oid")));
        return DimensionHoursBucket.valueOf(dimension, getDateTime(document));
    }

    private static Date getDateTime(Document document) {
        return document.getDate(DATE_TIME_KEY);
    }

}
