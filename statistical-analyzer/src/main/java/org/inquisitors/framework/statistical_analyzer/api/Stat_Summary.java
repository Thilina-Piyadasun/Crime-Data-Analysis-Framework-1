package org.inquisitors.framework.statistical_analyzer.api;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.mllib.linalg.Vector;
import org.inquisitors.framework.statistical_analyzer.beans.frequentCrimePatterns;
import org.inquisitors.framework.statistical_analyzer.beans.frequentCrimeSet;

import java.util.List;

/**
 * Created by Thilina
 * Overall description about the dataset
 */
public interface Stat_Summary {

    /*
    * give overall results and summeries of given colomns
    */
   /* Vector getMean(DataFrame df);

    Vector getVariance(DataFrame df);

    *//*
    * get count of non zero values of each coloumn
    * *//*
    Vector getNonZeroes(DataFrame df);
*/
    /*
    * get correlation of given data set
    *   "pearson" or "spearman"
    * */
  //  org.apache.spark.mllib.linalg.Matrix getCorrelation(DataFrame df,CorrelationMethod method);

    /*
    * get Covariance of two colomns
    * */
    double getCovariance(String col1, String col2);
    /*
     * get correlation of two colomns
     * */
    double getCorrelation(String col1, String col2, CorrelationMethod method);

    /*
    * frequent item sets in given data set
    * select convenient return type instead of Data frame
    * */
   /* ArrayList<ArrayList> getFrequentItems(DataFrame df,String col[],double support);*/

    List<frequentCrimeSet> frequentItemSets(DataFrame df,double minSupport);

    List<frequentCrimePatterns> mineFrequentPatterns(DataFrame df,double minSupport,double conf);
}
