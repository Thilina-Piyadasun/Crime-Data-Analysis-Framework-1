package org.inquisitors.framework.front_connector.api;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 * Created by Thilina on 8/12/2016.
 */
public interface Config extends Serializable {

    /*
      *  set hadoop home property path (in windows)
    */
    void setHadoopHome(String hadoop_home);


}
