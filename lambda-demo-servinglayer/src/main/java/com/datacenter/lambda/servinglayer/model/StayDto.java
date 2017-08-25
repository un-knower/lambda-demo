package com.datacenter.lambda.servinglayer.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

import static java.math.RoundingMode.HALF_EVEN;

/**
 * Created by zuoc on 2017/7/27.
 */
public class StayDto implements Serializable {

    private LocalDate date;

    private int registerAmount;

    private List<StayItem> stayItems;

    public static StayDto valueOf(LocalDate date, int registerAmount, List<StayItem> stayItems) {
        final StayDto stayDto = new StayDto();
        stayDto.date = date;
        stayDto.registerAmount = registerAmount;
        stayDto.stayItems = stayItems;
        return stayDto;
    }

    public static StayItem valueOf(int stayDay, int loginAmount, int registerAmount) {
        final StayItem stayItem = new StayItem();
        stayItem.stayDay = stayDay;
        stayItem.loginAmount = loginAmount;
        stayItem.stayRate = BigDecimal.valueOf(loginAmount).divide(BigDecimal.valueOf(registerAmount), 2, HALF_EVEN).doubleValue();
        return stayItem;
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public int getRegisterAmount() {
        return registerAmount;
    }

    public void setRegisterAmount(int registerAmount) {
        this.registerAmount = registerAmount;
    }

    public List<StayItem> getStayItems() {
        return stayItems;
    }

    public void setStayItems(List<StayItem> stayItems) {
        this.stayItems = stayItems;
    }

    @Override
    public String toString() {
        return "StayDto{" +
                "date=" + date +
                ", registerAmount=" + registerAmount +
                ", stayItems=" + stayItems +
                '}';
    }

    public static class StayItem implements Serializable {

        private int stayDay;

        private int loginAmount;

        private double stayRate;

        public int getStayDay() {
            return stayDay;
        }

        public void setStayDay(int stayDay) {
            this.stayDay = stayDay;
        }

        public int getLoginAmount() {
            return loginAmount;
        }

        public void setLoginAmount(int loginAmount) {
            this.loginAmount = loginAmount;
        }

        public double getStayRate() {
            return stayRate;
        }

        public void setStayRate(double stayRate) {
            this.stayRate = stayRate;
        }

        @Override
        public String toString() {
            return "StayItem{" +
                    "stayDay=" + stayDay +
                    ", loginAmount=" + loginAmount +
                    ", stayRate=" + stayRate +
                    '}';
        }
    }


}
