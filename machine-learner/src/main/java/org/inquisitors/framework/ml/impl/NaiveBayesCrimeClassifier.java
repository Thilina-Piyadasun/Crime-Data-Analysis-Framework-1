package org.inquisitors.framework.ml.impl;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.*;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.DataFrame;
import org.inquisitors.framework.ml.Beans.TestData;
import org.inquisitors.framework.ml.Beans.TrainData;
import org.inquisitors.framework.ml.api.Model;
import org.inquisitors.framework.ml.util.MLDataParser;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Vector;

/**
 * Created by User on 9/30/2016.
 */
public class NaiveBayesCrimeClassifier {

    String generated_feature_col_name="features";
    String indexedLabel="indexedLabel";
    String indexedFeatures="indexedFeatures";
    String predict ="prediction";
    private String[] featureCols=null;
    DataFrame trainingData;
    DataFrame testData;
    double partition;
    private MLDataParser dataParser=null;
    private String feature_VectorCol_name ="features";
    /*
    *   featuredDF - vector assemblered data frame to train the model
    *   label - label column in  train data set
    *   predictedLabel -a new column name to generate predictions to test data set and those predictions store under that column name
    *
    *   this method trains Random forest classifier model and return the model
    * */

    private PipelineModel train(DataFrame featuredDF, String label, String predictedLabel) {

        try{
            StringIndexerModel labelIndexer = new StringIndexer()
                    .setInputCol(label)
                    .setOutputCol(indexedLabel)
                    .fit(featuredDF);
            // Automatically identify categorical features, and index them.
            // Set maxCategories so features with > 4 distinct values are treated as continuous.
            VectorIndexerModel featureIndexer = new VectorIndexer()
                    .setInputCol("features")
                    .setOutputCol(indexedFeatures)
                    .setMaxCategories(4)
                    .fit(featuredDF);

            NaiveBayes rf = new NaiveBayes()
                    .setLabelCol(indexedLabel)
                    .setFeaturesCol(indexedFeatures);

            // Convert indexed labels back to original labels.
            IndexToString labelConverter = new IndexToString()
                    .setInputCol(predict)
                    .setOutputCol(predictedLabel)
                    .setLabels(labelIndexer.labels());

            Pipeline pipeline = new Pipeline()
                    .setStages(new PipelineStage[]{labelIndexer, featureIndexer, rf, labelConverter});

            PipelineModel model = pipeline.fit(featuredDF);
            return model;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public DataFrame train_and_Predict(TrainData trainData, TestData testData)throws Exception{

        String label=trainData.getLabel();
        String predictedLabel =testData.getPrediction_coloumn();

        DataFrame featuredDF=getFeaturesFrame(trainData.getTrainDF(), trainData.getFeature_columns());
        System.out.println("featureDFs");
        featuredDF.show(20);
        DataFrame featuredTestData=getFeaturesFrame(testData.getTestDF(), trainData.getFeature_columns());

        new VectorIndexer()
                .setInputCol(generated_feature_col_name)
                .setOutputCol(indexedFeatures)
                .setMaxCategories(4)
                .fit(featuredTestData);


        PipelineModel model=train(featuredDF,label,predictedLabel);
        // Make predictions.
        DataFrame predictions = model.transform(featuredTestData);
        predictions.show(10);
        // Select example row-s to display.
        predictions.select(predictedLabel, "features").show(50);
        return predictions;
    }


    /*
    * Use only train set to train model and get accuracy
    * */
    public MulticlassMetrics evaluateModel(TrainData dataFrame, double partition)throws Exception{

        featureCols=dataFrame.getFeature_columns();
        DataFrame df=dataFrame.getTrainDF();
        df=indexingColumns(df);
        df.show(20);
        DataFrame output=getFeaturesFrame(df, featureCols);

        String label=dataFrame.getLabel();
        String predictedLabel ="predictedLabel";

        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol(label)
                .setOutputCol("indexedLabel")
                .fit(output);
        // Automatically identify categorical features, and index them.
        // Set maxCategories so features with > 4 distinct values are treated as continuous.
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(4)
                .fit(output);

        DataFrame[] splits = output.randomSplit(new double[] {0.8, 0.2});
        DataFrame trainingData = splits[0];
        DataFrame testData = splits[1];

        NaiveBayes rf = new NaiveBayes()
                .setLabelCol(indexedLabel)
                .setFeaturesCol(indexedFeatures);

        // Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol(predictedLabel)
                .setLabels(labelIndexer.labels());

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {labelIndexer, featureIndexer, rf, labelConverter});

        PipelineModel model = pipeline.fit(trainingData);

        if(model!=null){
            DataFrame predictions = model.transform(testData);

            predictions.select("prediction","categoryIndex").show(100);

            DataFrame predictionAndLabels = predictions.select("prediction", "indexedLabel");
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
            throw new Exception("no trained randomForest classifier model found");
        }
    }

    /*
    * generate extra feature vector column to given dataset
    * */
    public DataFrame getFeaturesFrame(DataFrame df,String[] featureCols){

        return new MLDataParser().getFeaturesFrame(df,featureCols, generated_feature_col_name);
    }

    public DataFrame predict(Vector<Object> features) {
        return null;
    }

    public Vector evaluate() {
        return null;
    }

    public Model setGISData(HashMap<String, Vector> dataSet) {


        return null;
    }

    public Model setWeatherData(HashMap<String, Vector> dataSet) {
        return null;
    }

    public DataFrame train_and_Predict1(TrainData trainData,TestData testData)throws Exception{

        String label=trainData.getLabel();
        String predictedLabel =testData.getPrediction_coloumn();

        DataFrame featuredDF=getFeaturesFrame(trainData.getTrainDF(), trainData.getFeature_columns());
        System.out.println("featureDFs");
        featuredDF.show(20);
        DataFrame featuredTestData=getFeaturesFrame(testData.getTestDF(), trainData.getFeature_columns());

        new VectorIndexer()
                .setInputCol("features")
                .setOutputCol(indexedFeatures)
                .setMaxCategories(4)
                .fit(featuredTestData);

        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("demnda")
                .setOutputCol(indexedLabel)
                .fit(featuredDF);
        // Automatically identify categorical features, and index them.
        // Set maxCategories so features with > 4 distinct values are treated as continuous.
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol(indexedFeatures)
                .setMaxCategories(4)
                .fit(featuredDF);

        NaiveBayes rf = new NaiveBayes()
                .setLabelCol(indexedLabel)
                .setFeaturesCol(indexedFeatures);

        // Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol(predict)
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{labelIndexer, featureIndexer, rf, labelConverter});

        PipelineModel model = pipeline.fit(featuredDF);

        // Make predictions.
        DataFrame predictions = model.transform(featuredTestData);

        // Select example row-s to display.
        predictions.select("predictedLabel", "features").show(50);
        return predictions;
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

}
