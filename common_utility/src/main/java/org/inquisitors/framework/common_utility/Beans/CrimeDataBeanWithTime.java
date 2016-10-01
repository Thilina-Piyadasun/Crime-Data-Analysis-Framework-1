package org.inquisitors.framework.common_utility.Beans;

import java.io.Serializable;

/**
 * Created by Thilina on 8/17/2016.
 */
public class CrimeDataBeanWithTime implements Serializable {

    private int TimeIndex;
    private String DayOfWeek;
    private String Category;
    private String PdDistrict;
    private String Resolution;
    private double X;
    private double Y;

    public CrimeDataBeanWithTime(int time, String category, String dayOfWeek, String pdDistrict,String resolution, double x, double y) {
        this.TimeIndex =time;
        this.DayOfWeek = dayOfWeek;
        this.Category = category;
        this.PdDistrict = pdDistrict;
        this.Resolution=resolution;
        this.X = x;
        this.Y = y;
    }

    public String getDayOfWeek() {
        return DayOfWeek;
    }

    public void setDayOfWeek(String dayOfWeek) {
        this.DayOfWeek = dayOfWeek;
    }

    public String getCategory() {
        return Category;
    }

    public void setCategory(String category) {
        this.Category = category;
    }

    public String getPdDistrict() {
        return PdDistrict;
    }

    public void setPdDistrict(String pdDistrict) {
        this.PdDistrict = pdDistrict;
    }

    public double getX() {
        return X;
    }

    public void setX(double x) {
        this.X = x;
    }

    public double getY() {
        return Y;
    }

    public void setY(double y) {
        this.Y = y;
    }

    public void setTimeIndex(int timeIndex) {
        TimeIndex = timeIndex;
    }

    public void setResolution(String resolution) {
        Resolution = resolution;
    }

    public int getTimeIndex() {
        return TimeIndex;
    }

    public String getResolution() {
        return Resolution;
    }
}
