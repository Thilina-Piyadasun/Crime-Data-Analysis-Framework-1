package org.inquisitors.framework.front_connector.impl;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.inquisitors.framework.common_utility.Beans.HistogramBean;
import org.inquisitors.framework.common_utility.Config;
import org.inquisitors.framework.common_utility.CrimeUtil;
import org.inquisitors.framework.statistical_analyzer.api.StatisticalAnalyzer;
import org.inquisitors.framework.statistical_analyzer.impl.StatisticalAnalyzerCore;
import org.inquisitors.framework.statistical_analyzer.utils.Converter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Thilina on 8/28/2016.
 */
public class StatAnalyzer implements Serializable {

    private Converter converter;
    StatisticalAnalyzer statAnalyzerCore;

    //time wise data filtering
    public List<HistogramBean> timeWiseData(DataFrame df,int timeFrom,int timeTo,String col){
            try{
            statAnalyzerCore=new StatisticalAnalyzerCore();
            DataFrame dataFrame=statAnalyzerCore.timeWiseData(df, timeFrom, timeTo);

            DataFrame dataFrame1= dataFrame.select(col);

            dataFrame1.registerTempTable("timeData");
            DataFrame histo=Config.getSqlContext().sql("select "+col+ " ,count(*) from timeData group by "+col);
            histo.show(50);
            CrimeUtil crimeUtil=new CrimeUtil();

            List<HistogramBean> list=crimeUtil.getVisualizeList(histo);
            System.out.println(list.toString());
            return list;

        }
        catch (Exception e){
            e.printStackTrace();
        }
        return  null;
    }

    //categorywise data filtering
    public List<HistogramBean> categoryWiseData(DataFrame df,String category,String col){

        statAnalyzerCore=new StatisticalAnalyzerCore();
        DataFrame dataFrame=statAnalyzerCore.categoryWiseColData(df, category, col);

        dataFrame.registerTempTable("categoryData");
        DataFrame histo=Config.getSqlContext().sql("select "+col+ " ,count(*) from categoryData group by "+col);
        histo.show(50);
        CrimeUtil crimeUtil=new CrimeUtil();

        List<HistogramBean> list=crimeUtil.getVisualizeList(histo);
        System.out.println(list.toString());
        return list;
    }

    public void multipleCategoryWiseData(DataFrame df,String[] categories){

        statAnalyzerCore=new StatisticalAnalyzerCore();
        DataFrame dataFrame=statAnalyzerCore.categoryWiseData(df, categories);
        dataFrame.show(50);



    }
}
