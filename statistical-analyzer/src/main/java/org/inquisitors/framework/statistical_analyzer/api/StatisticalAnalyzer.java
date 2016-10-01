package org.inquisitors.framework.statistical_analyzer.api;

import org.apache.spark.sql.DataFrame;
import org.inquisitors.framework.statistical_analyzer.beans.DataSummary;

import java.util.List;

/**
 * Created by Thilina
* Analyze the Relationship between Category , PoliceJuri, location and time
* */
public interface StatisticalAnalyzer {
    DataSummary getSummary(DataFrame dataFrame, String baseField, String baseClass);

    List getAllFields(DataFrame dataFrame);

    List getSubFields(DataFrame dataFrame,String baseField);

    DataFrame categoryWiseColData(DataFrame df,String cat,String col);

    DataFrame categoryWiseData(DataFrame df,String[] categories);

    DataFrame timeWiseData(DataFrame df,int timeFrom,int timeTo);


}
