package com.datacenter.lambda.common;

/**
 * Created by zuoc on 2017/7/24.
 */
public enum  DimensionEnum {

    ALL, OPERATOR, SERVER, APP;

    public static String all() {
        return "_";
    }
}
