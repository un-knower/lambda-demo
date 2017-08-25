package com.datacenter.lambda.servinglayer.model;

import java.io.Serializable;

/**
 * Created by zuoc on 2017/7/27.
 */
public class AnalysisResult implements Serializable {

    private Object offline;

    private Object realtime;

    public static AnalysisResult offline(Object offline) {
        final AnalysisResult result = new AnalysisResult();
        result.offline = offline;
        return result;
    }

    public static AnalysisResult realtime(Object realtime) {
        final AnalysisResult result = new AnalysisResult();
        result.realtime = realtime;
        return result;
    }

    public static AnalysisResult combine(Object offline, Object realtime) {
        final AnalysisResult result = new AnalysisResult();
        result.offline = offline;
        result.realtime = realtime;
        return result;
    }

    public Object getOffline() {
        return offline;
    }

    public void setOffline(Object offline) {
        this.offline = offline;
    }

    public Object getRealtime() {
        return realtime;
    }

    public void setRealtime(Object realtime) {
        this.realtime = realtime;
    }

    @Override
    public String toString() {
        return "AnalysisResult{" +
                "offline=" + offline +
                ", realtime=" + realtime +
                '}';
    }
}
