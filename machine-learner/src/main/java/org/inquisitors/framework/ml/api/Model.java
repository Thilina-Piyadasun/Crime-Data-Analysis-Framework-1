package org.inquisitors.framework.ml.api;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.DataFrame;
import java.util.HashMap;
import java.util.Vector;

/**
 * Created by minudika on 8/12/16.
 */
public interface Model {

 //   Model train(DataFrame dataFrame);

    DataFrame train(DataFrame dataFrame, String predictingColumn,String[] relatedFatureColomns);

    DataFrame train_and_Predict(Vector<Object>features);

    double evaluateModel();

    Model setGISData(HashMap<String,Vector> dataSet);

    Model setWeatherData(HashMap<String,Vector> dataSet);
}