package org.inquisitors.framework.statistical_analyzer.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.inquisitors.framework.common_utility.Config;
import org.inquisitors.framework.common_utility.Beans.CrimeDataBean;

import java.io.Serializable;

/**
 * Created by Thilina on 8/26/2016.
 */
public class StatDataConverter implements Serializable {

    public  JavaRDD<CrimeDataBean> getCrimeRDD(DataFrame df){

        try{
            Config insance= Config.getInstance();
            SQLContext sqlContext= insance.getSqlContext();

            DataFrame rdd=sqlContext.sql("Select Dates,DayOfWeek,PdDistrict,Category,X,Y from crimeData");
            JavaRDD<CrimeDataBean> crimeDataBeanJavaRDD = rdd.javaRDD().map(new Function<Row, CrimeDataBean>() {
                public CrimeDataBean call(Row row) {
                    CrimeDataBean crimeDataBean = new CrimeDataBean(row.getTimestamp(0),row.getString(1),row.getString(2),row.getString(3),row.getDouble(4),row.getDouble(5));
                    return crimeDataBean;
                }
            });
            return crimeDataBeanJavaRDD;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return  null;
    }

    public static JavaRDD<Vector> getCrimeDataVector(DataFrame df){

        try{
            Config insance=Config.getInstance();
            SQLContext sqlContext= insance.getSqlContext();

            DataFrame rdd=sqlContext.sql("Select Dates,DayOfWeek,PdDistrict,Category,X,Y from crimeData");

            JavaRDD<Vector> crimeDataBeanJavaRDD = rdd.javaRDD().map(new Function<Row, Vector>() {
                public Vector call(Row row) {
                    return Vectors.dense(row.getDouble(4), row.getDouble(5));
                }
            });
            return crimeDataBeanJavaRDD;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return  null;
    }
}
