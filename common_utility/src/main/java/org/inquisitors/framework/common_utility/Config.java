package org.inquisitors.framework.common_utility;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by Thilina on 8/27/2016.
 */
public class Config {

    private static SparkConf conf ;
    private static JavaSparkContext sc ;
    private static SQLContext sqlContext ;
    private static Config instance;


    private Config(){}

    public static Config getInstance(){
        if(instance==null){
            instance=new Config();
        }
        return instance;
    }

    public  static void configure(){

        conf=new SparkConf().setMaster("local").setAppName("DataReader");
        sc=new JavaSparkContext(conf);
        sqlContext= new org.apache.spark.sql.SQLContext(sc);

    }

    public void setHadoopHome(String hadoop_home){
        System.setProperty("hadoop.home.dir", hadoop_home);
    }

    public static SparkConf getConf() {
        conf = new SparkConf().setMaster("local").setAppName("DataReader");
        return conf;
    }

    public static JavaSparkContext getSc() {
        return sc;
    }

    public static SQLContext getSqlContext() {
        return sqlContext;
    }


}
