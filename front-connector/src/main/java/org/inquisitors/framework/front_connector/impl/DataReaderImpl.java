package org.inquisitors.framework.front_connector.impl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.inquisitors.framework.common_utility.Config;
import org.inquisitors.framework.common_utility.Beans.CrimeDataBean;

import java.io.Serializable;

/**
 * Created by Thilina Piyadasun on 7/3/2016.
 * Read the given file and cache the content
 */
public class DataReaderImpl implements Serializable{

    //static JavaRDD<String> input;
    Config config;
    public static DataFrame input;
    public static SparkConf conf ;

    public static JavaSparkContext sc ;
    public static SQLContext sqlContext ;

    /*
    * Read user file and cache the content in RDD
    * int storage level specifies the level of Storage.(MEMORY_ONLY,MEMORY_ONLY_SER ,MEMORY_AND_DISK, MEMORY_AND_DISK_SER)
    *
    */
    public void read_file(String filename ,int storage_level){

        conf = new SparkConf().setMaster("local").setAppName("DataReader");
        sc  = new JavaSparkContext(conf);
        sqlContext =  new org.apache.spark.sql.SQLContext(sc);


        // Load the input data to a static Data Frame
        input=sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header","true")
                .option("inferSchema","true")
                .load(filename);

        input.registerTempTable("crimeData");
        cache_data(storage_level);
    }

    private void cache_data(int storage_level){

        if(storage_level==1)
            input.persist(StorageLevel.MEMORY_ONLY());
        else if(storage_level==2)
            input.persist(StorageLevel.MEMORY_ONLY_SER());
        else if(storage_level==3)
            input.persist(StorageLevel.MEMORY_AND_DISK());
        else
            input.persist(StorageLevel.MEMORY_AND_DISK_SER());
    }

    public static JavaRDD<CrimeDataBean> getCrimeRDD(){

        try{
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

    public static JavaRDD<Vector> getCrimeDataVector(){

        try{
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
