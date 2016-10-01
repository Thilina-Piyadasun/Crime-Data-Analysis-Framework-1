package org.inquisitors.framework.ml.util;

import org.apache.spark.ml.feature.QuantileDiscretizer;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.DataFrame;
import org.inquisitors.framework.common_utility.CrimeUtil;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by Thilina on 8/12/2016.
 * This class convert data into standard formats
 */
public class MLDataParser implements Serializable{

    //preprocessed dataframe.
    //must set methods to set dataframe after preprocess

    public DataFrame getFeaturesFrame(DataFrame df,String[] inputColumns,String outputColumnName){

        try {


            df=concatGISData(df);
            df=concatGISData(df);
            /*
            * for eg :
            *
            * VectorAssembler assembler2 = new VectorAssembler()
                .setInputCols(new String[]{"agencia_ID", "canal_ID", "ruta_SAK", "cliente_ID", "producto_ID"})
                .setOutputCol("features");
            * */
            VectorAssembler vectorAssembler = new VectorAssembler()
                    .setInputCols(inputColumns)
                    .setOutputCol(outputColumnName);

            DataFrame featuredDF = vectorAssembler.transform(df);
            System.out.println("=======================================");
            System.out.println("Data frame Featured");
            System.out.println("=======================================");
            featuredDF.show(10);

            return featuredDF;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /*
    *  This method can use to Standardise the data set.
    *  input Col should be a Featured Dataframe
    *
    */
    public DataFrame standardiseData(DataFrame featuredDF, String inputCol, String outputCol){
        try {
            StandardScaler scaler = new StandardScaler()
                    .setInputCol(inputCol)
                    .setOutputCol(outputCol)
                    .setWithStd(true)
                    .setWithMean(true);

            DataFrame standardisedDf = scaler.fit(featuredDF).transform(featuredDF);
            return standardisedDf;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }


    public DataFrame discritizeColoumn(DataFrame dataFrame,String inputCol,int noOfBuckets,String outputColName){
        try {


            QuantileDiscretizer discretizer = new QuantileDiscretizer()
                    .setInputCol(inputCol)
                    .setOutputCol(outputColName)
                    .setNumBuckets(3);
            return discretizer.fit(dataFrame).transform(dataFrame);
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return dataFrame;
    }

    public DataFrame indexColumn(DataFrame df,String colName ,String indexedColName){

        try{
            CrimeUtil crimeUtil=new CrimeUtil();
            boolean colexists=crimeUtil.isColExists(df,colName);

            if(colexists){
                StringIndexer indexer = new StringIndexer()
                        .setInputCol(colName)
                        .setOutputCol(indexedColName);

                DataFrame indexed = indexer.fit(df).transform(df);
                return indexed;
            }

        }catch (Exception e){
            e.printStackTrace();
        }
        return df;
    }

    public DataFrame concatGISData(DataFrame df){
        //TODO get GIS data and convert it to a vector and merge that vector col with df (here you must add GIS_data col name for String[] feature_columns in df).
        //then pass it to ML data paser adn get featuredDF and train the model
        return df;
    }
    public DataFrame concatWeatherDta(DataFrame df){
        //TODO get GIS data and convert it to a vector and merge that vector col with df (here you must add GIS_data col name for String[] feature_columns in df).
        //then pass it to ML data paser adn get featuredDF and train the model
        return df;
    }
}
