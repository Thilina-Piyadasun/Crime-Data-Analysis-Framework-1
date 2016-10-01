package org.inquisitors.framework.front_connector.application;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.inquisitors.framework.common_utility.Config;

/**
 * Created by Thilina on 8/26/2016.
 */
public class DataReader {


    public static DataFrame input;
    public static SparkConf conf ;

    public static JavaSparkContext sc ;
    public static SQLContext sqlContext ;

    /*
    * Read user file and cache the content in RDD
    * int storage level specifies the level of Storage.(MEMORY_ONLY,MEMORY_ONLY_SER ,MEMORY_AND_DISK, MEMORY_AND_DISK_SER)
    *
    */
    public DataFrame readCSV(String filename ,int storage_level){


        sqlContext =  Config.getSqlContext();


        // Load the input data to a static Data Frame
        input=sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header","true")
                .option("inferSchema","true")
                .load(filename);
        input.show(20);
        input.registerTempTable("crimeData");
        cache_data(storage_level);
        return input;
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
}
