package org.inquisitors.framework.statistical_analyzer.impl;

/**
 * Created by minudika on 8/6/16.
 */

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.inquisitors.framework.common_utility.Config;
import org.inquisitors.framework.common_utility.CrimeUtil;
import org.inquisitors.framework.statistical_analyzer.api.StatisticalAnalyzer;
import org.inquisitors.framework.statistical_analyzer.utils.Converter;
import org.inquisitors.framework.statistical_analyzer.beans.DataSummary;

import java.util.ArrayList;
import java.util.List;

public class StatisticalAnalyzerCore implements StatisticalAnalyzer {


    private SQLContext sqlContext ;
    private CrimeUtil crimeUtil;
    /*get data crime categorywise
    * this gives data to implement categorywise heat map
    * */
    public DataFrame categoryWiseData(DataFrame df,String[] categories){

        try {

            sqlContext= Config.getSqlContext();
            crimeUtil=new CrimeUtil();
            df=crimeUtil.getTimeIndexedDF(df, "Dates");

            String s = " ";
            for (int i = 0; i < categories.length; i++) {
                if (i == 0)
                    s = s + " where ";

                s = s + "category=" + "'" + categories[i] + "'";
                if (i + 1 != categories.length)
                    s = s + " or ";
            }

            df.registerTempTable("DataTbl");
            DataFrame dataFrame = sqlContext.sql("Select * from DataTbl " + s);
            return dataFrame;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return  null;
    }

    /*get data crime categorywise
       * this gives data to implement categorywise heat map
       * */
    public DataFrame categoryWiseColData(DataFrame df,String cat,String col){

        try {

            sqlContext= Config.getSqlContext();
            crimeUtil=new CrimeUtil();
            df=crimeUtil.getTimeIndexedDF(df, "Dates");

            df.registerTempTable("DataTbl");
            DataFrame dataFrame = sqlContext.sql("Select "+col+" from DataTbl where category="+"'"+cat+"'");
            dataFrame.show(30);
            return dataFrame;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return  null;
    }

    public DataFrame timeWiseData(DataFrame df,int timeFrom,int timeTo){

        //convert Date to 1-24 time hours
        CrimeUtil cu=new CrimeUtil();
        df=cu.getTimeIndexedDF(df, "Dates");

        try {

            sqlContext= Config.getSqlContext();

            df.registerTempTable("DataTbl");
            DataFrame dataFrame = sqlContext.sql("Select * from DataTbl where time between '"+timeFrom+"' AND '"+timeTo+"'");
            dataFrame.show(50);
            return dataFrame;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return  null;
    }


    public List getAllFields(DataFrame dataFrame) {
        String str = dataFrame.collectAsList().toString();
        String[] FnTs = str.substring(1,str.length()-1).split(",");
        List<String> fields = new ArrayList();
        for(String s : FnTs){
            fields.add(s.split(":")[1].trim().toString());
        }
        return fields;
    }

    public List getSubFields(DataFrame dataFrame, String baseField) {
        String str = dataFrame.collectAsList().toString();
        String[] FnTs = str.substring(1,str.length()-1).split(",");
        List<String> fields = new ArrayList();
        for(String s : FnTs){
            fields.add(s.split(":")[1].trim().toString());
        }
        fields.remove((Object)baseField);
        return fields;
    }

    public DataSummary getSummary(DataFrame dataFrame,String baseField,String baseClass){
        DataSummary dataSummary = new DataSummary(baseField);
        dataSummary.setRecords(summarize(dataFrame,baseField,baseClass));
        return dataSummary;
    }

    private List<ArrayList> summarize(DataFrame dataFrame,String baseField,String baseClass){;

        sqlContext=Config.getSqlContext();
        List<String> subFields = new ArrayList<String>();

        dataFrame.registerTempTable("dataset");
        dataFrame.show(30);
        StringBuilder stringBuilder = new StringBuilder();
        for(String field : subFields){
            stringBuilder.append(field+",");
        }

        if(stringBuilder.length()>0) {
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        }

        String sqlQuery = "SELECT " + stringBuilder.toString() + " FROM dataset " +
                " WHERE " + baseField + " = " + "'"+baseClass+"'";

        List<ArrayList> list;
        DataFrame df = sqlContext.sql(sqlQuery);
        Converter converter = new Converter();
        list = converter.convert(df);

        return list;
    }
}
