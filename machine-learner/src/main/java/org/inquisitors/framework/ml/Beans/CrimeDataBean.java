package org.inquisitors.framework.ml.Beans;

/**
 * Created by Thilina on 8/17/2016.
 */
public class CrimeDataBean {

    private int time;
    private String dayOfWeek;
    private String category;
    private String pdDistrict;
    private double x;
    private double y;

    public CrimeDataBean( String category,String dayOfWeek, String pdDistrict, double x, double y) {

        this.dayOfWeek = dayOfWeek;
        this.category = category;
        this.pdDistrict = pdDistrict;
        this.x = x;
        this.y = y;
    }

    public String getDayOfWeek() {
        return dayOfWeek;
    }

    public void setDayOfWeek(String dayOfWeek) {
        this.dayOfWeek = dayOfWeek;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getPdDistrict() {
        return pdDistrict;
    }

    public void setPdDistrict(String pdDistrict) {
        this.pdDistrict = pdDistrict;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }
}
