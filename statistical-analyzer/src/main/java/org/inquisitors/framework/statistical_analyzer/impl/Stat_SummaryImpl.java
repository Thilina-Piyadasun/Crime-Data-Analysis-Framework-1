package org.inquisitors.framework.statistical_analyzer.impl;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.inquisitors.framework.common_utility.Config;
import org.inquisitors.framework.statistical_analyzer.api.CorrelationMethod;
import org.inquisitors.framework.statistical_analyzer.api.Stat_Summary;
import org.inquisitors.framework.statistical_analyzer.beans.frequentCrimePatterns;
import org.inquisitors.framework.statistical_analyzer.beans.frequentCrimeSet;
import org.inquisitors.framework.statistical_analyzer.utils.StatDataConverter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Thilina on 8/5/2016.
 */
public class Stat_SummaryImpl implements Stat_Summary ,Serializable{


    StatDataConverter statDataConverter=new StatDataConverter();
    SQLContext sqlContext;

    /*
    * Frequent Crime Sets in given data frame eg : [VehiclTheft][Assults ,Monday]
    * */
    public List<frequentCrimeSet> frequentItemSets(DataFrame df,double minSupport){

        List<frequentCrimeSet> frequentCrimeSets =new ArrayList<>();
        //get <List<String>> type RDD
        JavaRDD<List<String>> transactions =getTempRDDForFPGrowth(df);

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(minSupport)
                .setNumPartitions(4);
        FPGrowthModel<String> model = fpg.run(transactions);

        for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
            frequentCrimeSet record=new frequentCrimeSet(itemset.javaItems(),itemset.freq());

            frequentCrimeSets.add(record);
        }
        return frequentCrimeSets;
    }

    public List<frequentCrimePatterns> mineFrequentPatterns(DataFrame df ,double minSupport,double conf) {

        List<frequentCrimePatterns> frequentCrimeRules =new ArrayList<>();
        //get <List<String>> type RDD
        JavaRDD<List<String>> transactions =getTempRDDForFPGrowth(df);

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(minSupport)
                .setNumPartitions(4);
        FPGrowthModel<String> model = fpg.run(transactions);

        for (AssociationRules.Rule<String> rule : model.generateAssociationRules(conf).toJavaRDD().collect()) {
            System.out.println(
                    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());

            frequentCrimePatterns rules=new frequentCrimePatterns(rule.javaAntecedent(),rule.javaConsequent(),rule.confidence());
            frequentCrimeRules.add(rules);
        }
        return frequentCrimeRules;
    }

    /*public Vector getMean(DataFrame df) {
        CrimeDataHolder crimeDataHolder= CrimeDataHolder.getCrimeDataHolder();
        MultivariateStatisticalSummary summary = Statistics.colStats(statDataConverter.getCrimeDataVector(df).rdd());
        return summary.mean();
    }

    public Vector getVariance(DataFrame df) {
        CrimeDataHolder crimeDataHolder= CrimeDataHolder.getCrimeDataHolder();
        MultivariateStatisticalSummary summary = Statistics.colStats(statDataConverter.getCrimeDataVector(df).rdd());
        return summary.variance();
    }

    public Vector getNonZeroes(DataFrame df) {
        CrimeDataHolder crimeDataHolder= CrimeDataHolder.getCrimeDataHolder();
        MultivariateStatisticalSummary summary = Statistics.colStats(statDataConverter.getCrimeDataVector(df).rdd());
        return summary.variance();

    }

    public Matrix getCorrelation(DataFrame df,CorrelationMethod method) {
        CrimeDataHolder crimeDataHolder= CrimeDataHolder.getCrimeDataHolder();
        Matrix correlMatrix = Statistics.corr(statDataConverter.getCrimeDataVector(df).rdd(), method.toString());
        return correlMatrix;
    }
*/
    public double getCovariance(String col1, String col2){
        //TODO:Implement after preprocessor done
        return 0;
    }

    public double getCorrelation(String col1, String col2, CorrelationMethod method) {
        //TODO:Implement after preprocessor done
        return  0;

    }


    private JavaRDD<List<String>> getTempRDDForFPGrowth(DataFrame df) {
        //TODO:Exception handel
        //TODO:
        sqlContext= Config.getSqlContext();
        df.registerTempTable("freqtable");
        df.show(40);

        //TODO: add time col after preporessor done
        DataFrame selected=sqlContext.sql("select category,dayOfWeek,pdDistrict,x,y,time from freqtable");

        JavaRDD<List<String>> transactions = selected.javaRDD().map(new Function<Row, List<String>>() {
            public List<String> call(Row row) {
                String[] items = {row.getString(0), row.getString(1),row.getString(2),""+ row.getDouble(3),""+row.getDouble(4)+""+row.getInt(5)};
                return Arrays.asList(items);
            }
        });

        return transactions;
    }

     /*public ArrayList<ArrayList> getFrequentItems(DataFrame df,String[] col, double support) {

        DataFrame frequntItemFrame=df;
        frequntItemFrame.stat().freqItems(col,support);

        DataFrame temp= frequntItemFrame.stat().freqItems(col,support);
        temp.show(10);
        Converter converter=new Converter();
        return converter.convert(temp);
    }*/
}
