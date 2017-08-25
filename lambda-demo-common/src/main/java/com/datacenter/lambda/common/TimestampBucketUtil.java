package com.datacenter.lambda.common;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

/**
 * Created by zuoc on 2017/7/20.
 */
public class TimestampBucketUtil {

    private static final int HOUR_SECS = 60 * 60;

    private static final int DAY_SECS = HOUR_SECS * 24;

    public static int toHourBucket(Date time) {
        return (int) (time.toInstant().getEpochSecond() / HOUR_SECS);
    }

    public static int toHourBucket(LocalDateTime time) {
        return (int) (time.atZone(ZoneId.systemDefault()).toInstant().getEpochSecond() / HOUR_SECS);
    }


    public static LocalDateTime ofHourBucket(int hourBucket) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(hourBucket * HOUR_SECS), ZoneId.systemDefault());
    }

    public static int toDayBucket(Date time) {
        return (int) (time.toInstant().getEpochSecond() / DAY_SECS);
    }

    public static LocalDate ofDayBucket(int dayBucket) {
        return LocalDate.ofEpochDay(dayBucket);
    }

    public static void main(String[] args) {
        System.out.println(TimestampBucketUtil.toHourBucket(new Date()));
        System.out.println(TimestampBucketUtil.toHourBucket(LocalDateTime.now()));
    }
}
