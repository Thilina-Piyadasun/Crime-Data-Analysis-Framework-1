package org.inquisitors.framework.ml.Beans;

import org.apache.spark.sql.DataFrame;

import java.io.Serializable;

/**
 * Created by Thilina on 8/15/2016.
 */
public class TrainData implements Serializable{

    private DataFrame trainDF;
    private String[] all_columns;
    private String[] feature_columns;
    private String label;

    public TrainData(DataFrame trainDF, String[] all_columns, String[] feature_columns, String label) {
        this.trainDF = trainDF;
        this.all_columns = all_columns;
        this.feature_columns = feature_columns;
        this.label = label;
    }

    public TrainData(DataFrame trainDF, String[] feature_columns, String label) {
        this.trainDF = trainDF;
        this.feature_columns = feature_columns;
        this.label = label;
    }

    public DataFrame getTrainDF() {
        return trainDF;
    }

    public void setTrainDF(DataFrame trainDF) {
        this.trainDF = trainDF;
    }

    public String[] getAll_columns() {
        return all_columns;
    }

    public void setAll_columns(String[] all_columns) {
        this.all_columns = all_columns;
    }

    public String[] getFeature_columns() {
        return feature_columns;
    }

    public void setFeature_columns(String[] feature_columns) {
        this.feature_columns = feature_columns;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }
}
