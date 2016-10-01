package org.inquisitors.framework.ml.Beans;

import org.apache.spark.sql.DataFrame;

import java.io.Serializable;

/**
 * Created by Thilina on 8/15/2016.
 */
public class TestData implements Serializable {

    private DataFrame testDF;
    private String[] all_colomns;
    private String[] feature_columns;
    private String prediction_coloumn;

    public TestData(DataFrame testDF, String[] all_colomns, String[] input_colomns, String prediction_coloumn) {
        this.testDF = testDF;
        this.all_colomns = all_colomns;
        this.feature_columns = input_colomns;
        this.prediction_coloumn = prediction_coloumn;
    }

    public TestData(DataFrame testDF, String[] input_colomns, String prediction_coloumn) {
        this.testDF = testDF;
        this.feature_columns = input_colomns;
        this.prediction_coloumn = prediction_coloumn;
    }

    public DataFrame getTestDF() {
        return testDF;
    }

    public void setTestDF(DataFrame testDF) {
        this.testDF = testDF;
    }

    public String[] getAll_colomns() {
        return all_colomns;
    }

    public void setAll_colomns(String[] all_colomns) {
        this.all_colomns = all_colomns;
    }

    public String[] getFeature_columns() {
        return feature_columns;
    }

    public void setFeature_columns(String[] feature_columns) {
        this.feature_columns = feature_columns;
    }

    public String getPrediction_coloumn() {
        return prediction_coloumn;
    }

    public void setPrediction_coloumn(String prediction_coloumn) {
        this.prediction_coloumn = prediction_coloumn;
    }
}
