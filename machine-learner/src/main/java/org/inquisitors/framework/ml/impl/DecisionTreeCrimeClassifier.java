package org.inquisitors.framework.ml.impl;

/**
 * Created by Thilina on 8/15/2016.
 */
public class DecisionTreeCrimeClassifier {

/*

    public DataFrame train_and_Predict(TrainData trainData,TestData testData)throws Exception{

        String label=trainData.getLabel();
        String predictedLabel =testData.getPrediction_coloumn();

        DataFrame featuredDF=getFeaturesFrame(trainData.getTrainDF(), trainData.getFeature_columns());
        PipelineModel model=train(featuredDF,label,predictedLabel);

        if(model!=null){

            DataFrame featuredTestData=getFeaturesFrame(testData.getTestDF(), trainData.getFeature_columns());
            DataFrame predictions = model.transform(featuredTestData);

            return predictions;
        }
        else {
            throw new Exception("no trained randomForest classifier model found");
        }

    }
    *//*
    * generate extra feature vector column to given dataset
    * *//*
    public DataFrame getFeaturesFrame(DataFrame df,String[] featureCols){

        return new MLDataParser().getFeaturesFrame(df,featureCols, feature_VectorCol_name);
    }*/
}
