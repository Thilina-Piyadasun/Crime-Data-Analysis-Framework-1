package org.inquisitors.framework.ml.api;

import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.sql.DataFrame;
import org.inquisitors.framework.ml.Beans.TestData;
import org.inquisitors.framework.ml.Beans.TrainData;

/**
 * Created by User on 9/30/2016.
 */
public interface PerceptionClassifierModel {

    DataFrame train_and_Predict(TrainData trainData,TestData testData,int[] layers,int blockSize,long seed,int maxIterations)throws Exception;

    MulticlassMetrics evaluateModel(TrainData dataFrame,double partition,int[] layers,int blockSize,long seed,int maxIterations)throws Exception;


}
