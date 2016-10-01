package org.inquisitors.framework.ml.impl;

import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.DataFrame;
import org.inquisitors.framework.ml.api.Model;
import org.inquisitors.framework.ml.api.PerceptionClassifierModel;
import org.inquisitors.framework.ml.util.MLDataParser;
import org.inquisitors.framework.ml.Beans.TestData;
import org.inquisitors.framework.ml.Beans.TrainData;
import scala.Tuple2;

/**
 * Created by Thilina on 8/17/2016.
 */

/*
* Classify data bases on feed forward artificial Neural Network
* */
public class PerceptionClassifier implements PerceptionClassifierModel {

    private MLDataParser dataParser=null;
    private String[] featureCols=null;
    private MultilayerPerceptronClassifier trainer;
    private String feature_VectorCol_name ="features";
    private String prediction="prediction";
    private String label;
    private boolean isStanderdize=false;

    private MultilayerPerceptronClassificationModel train(DataFrame featuredDF,int[] layers,int blockSize,long seed,int maxIterations) {

        try{
            trainer = new MultilayerPerceptronClassifier()
                    .setPredictionCol(prediction)
                    .setLabelCol(label)
                    .setFeaturesCol(feature_VectorCol_name)
                    .setLayers(layers)
                    .setBlockSize(blockSize)
                    .setSeed(seed)
                    .setMaxIter(maxIterations);
            // train the model
            MultilayerPerceptronClassificationModel model = trainer.fit(featuredDF);
            return  model;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }



    public DataFrame train_and_Predict(TrainData trainData,TestData testData,int[] layers,int blockSize,long seed,int maxIterations)throws Exception{

        label=trainData.getLabel();
        //set user given name to predicting column.default "prediction"
        prediction=testData.getPrediction_coloumn();
        featureCols=trainData.getFeature_columns();

        DataFrame trainDF=trainData.getTrainDF();
        DataFrame testDF=testData.getTestDF();

        trainDF=indexingColumns(trainDF);
        trainDF.show(20);

        testDF=indexingColumns(testDF);
        testDF.show(20);

        DataFrame FeaturedTrainSet=getFeaturesFrame(trainDF, featureCols);
        DataFrame FeaturedTestSet = getFeaturesFrame(testDF, featureCols);

        if (isStanderdize) {
            //stamderdise both train and tes test.
            FeaturedTrainSet= standerdizeFeaturesFrame(FeaturedTrainSet, feature_VectorCol_name, "std_" + feature_VectorCol_name);
            FeaturedTestSet = standerdizeFeaturesFrame(FeaturedTestSet, feature_VectorCol_name, "std_" + feature_VectorCol_name);
        }

        //receive generated model for forcsting
        MultilayerPerceptronClassificationModel model=train(FeaturedTrainSet,layers,blockSize,seed,maxIterations);
        if(model!=null){
            DataFrame predictions = model.transform(FeaturedTestSet);
            return predictions;
        }
        else {
            throw new Exception("no trained randomForest classifier model found");
        }

    }


   /* * Use only train set to train model and get accuracy
    * */
    public MulticlassMetrics evaluateModel(TrainData dataFrame,double partition,int[] layers,int blockSize,long seed,int maxIterations)throws Exception{

        label=dataFrame.getLabel();
        featureCols=dataFrame.getFeature_columns();
        DataFrame df=dataFrame.getTrainDF();

        //convert all string labeled columns into double values for classification
        df=indexingColumns(df);
        df.show(20);

        //featuring the dataframe
        DataFrame featuredDF=getFeaturesFrame(df,featureCols);
        featuredDF.show(20);

        /*if (isStanderdize) {
            featuredDF=standerdizeFeaturesFrame(featuredDF, feature_VectorCol_name, "std_" + feature_VectorCol_name);
        }*/

        DataFrame[] splits = featuredDF.randomSplit(new double[] {1-partition, partition});
        DataFrame trainingData = splits[0];
        DataFrame testData = splits[1];

        //receive generated model for forcsting
        MultilayerPerceptronClassificationModel model=train(trainingData,layers,blockSize,seed,maxIterations);

        if(model!=null){

            DataFrame predictions = model.transform(testData);
            predictions.show(30);

            DataFrame predictionAndLabels = predictions.select("prediction", "categoryIndex");
            MulticlassMetrics metrics= new MulticlassMetrics(predictionAndLabels) ;

            Matrix confusion=metrics.confusionMatrix();
            System.out.println("confusion metrix : \n" + confusion);
            System.out.println("Accuracy = " + metrics.precision());

            System.out.println("Precision of OTHER OFFENSES = " + metrics.precision(0));
            System.out.println("Recall of OTHER OFFENSES = " + metrics.recall(0));

            System.out.println("Precision of LARCENY/THEFT = " + metrics.precision(1));
            System.out.println("Recall of LARCENY/THEFT = " + metrics.recall(1));

            System.out.println("Precision of NON-CRIMINAL = " + metrics.precision(2));
            System.out.println("Recall of NON-CRIMINAL = " + metrics.recall(2));

            System.out.println("Precision of LARCENY/THEFT = " + metrics.precision(3));
            System.out.println("Recall of LARCENY/THEFT = " + metrics.recall(3));

            System.out.println("Precision of DRUG/NARCOTIC = " + metrics.precision(4));
            System.out.println("Recall of DRUG/NARCOTIC = " + metrics.recall(4));

            System.out.println("Precision of VEHICLE THEFT = " + metrics.precision(5));
            System.out.println("Recall of VEHICLE THEFT = " + metrics.recall(5));

            System.out.println("Precision of VANDALISM = " + metrics.precision(6));
            System.out.println("Recall of VANDALISM = " + metrics.recall(6));

            System.out.println("Precision of WARRANTS = " + metrics.precision(7));
            System.out.println("Recall of WARRANTS = " + metrics.recall(7));

            System.out.println("Precision of BURGLARY = " + metrics.precision(8));
            System.out.println("Recall of BURGLARY = " + metrics.recall(8));

            System.out.println("Precision of SUSPICIOUS OCC = " + metrics.precision(9));
            System.out.println("Recall of SUSPICIOUS OCC = " + metrics.recall(9));


            return metrics;
        }
        else {
            throw new Exception("no trained perception classifier model found");
        }
    }

    /*
    * generate extra feature vector column to given dataset
    * */
    public DataFrame getFeaturesFrame(DataFrame df,String[] featureCols){

        dataParser=new MLDataParser();
        return dataParser.getFeaturesFrame(df,featureCols, feature_VectorCol_name);
    }

    public DataFrame standerdizeFeaturesFrame(DataFrame featuredDF,String inputCol, String outputCol){

        if(dataParser==null){
            dataParser=new MLDataParser();
        }
        featuredDF = dataParser.standardiseData(featuredDF, feature_VectorCol_name, "std_" + feature_VectorCol_name);

        featuredDF = featuredDF.drop(feature_VectorCol_name);
        featuredDF = featuredDF.withColumnRenamed("std_" + feature_VectorCol_name, feature_VectorCol_name);
        featuredDF.show(20);
        System.out.println("Data standardization Completed");

        return featuredDF;
    }

    public void setIsStanderdize(boolean isStanderdize) {
        this.isStanderdize = isStanderdize;
    }

    /**
     * Convert all string type data columns in featured list into double valued columns
     */
    public DataFrame indexingColumns(DataFrame df){

        Tuple2<String,String>[] colTypes=df.dtypes();

        for(int j=0;j<featureCols.length;j++) {
            for (int i = 0; i < colTypes.length; i++) {

                if(colTypes[i]._2().equals("StringType")) {
                    if (featureCols[j].equals(colTypes[i]._1())) {

                        StringIndexer indexer2 = new StringIndexer()
                                .setInputCol(featureCols[j])
                                .setOutputCol(featureCols[j]+"Index");
                        featureCols[j]=featureCols[j]+"Index";
                        df = indexer2.fit(df).transform(df);
                    }
                }
            }
        }
        return df;
    }

}

