package org.inquisitors.framework.ml.impl;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.DataFrame;
import org.inquisitors.framework.ml.util.MLDataParser;
import org.inquisitors.framework.ml.Beans.TrainData;

/**
 * Created by Thilina on 8/17/2016.
 */
public class RandomForestAlt {


    String indexedLabel="indexedLabel";
    String indexedFeatures="indexedFeatures";
    String predict ="prediction";

    DataFrame trainingData;
    DataFrame testData;
    double partition;
    String generated_feature_col_name="features";

    public double evaluateModel(TrainData dataFrame,double partition)throws Exception{

        DataFrame output=getFeaturesFrame(dataFrame.getTrainDF(), dataFrame.getFeature_columns());
        DataFrame[] splits = output.randomSplit(new double[] {0.7, 0.3});
        DataFrame trainingData = splits[0];
        DataFrame testData = splits[1];

        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol(dataFrame.getLabel())
                .setOutputCol(indexedLabel)
                .fit(output);
        // Automatically identify categorical features, and index them.
        // Set maxCategories so features with > 4 distinct values are treated as continuous.
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol(generated_feature_col_name)
                .setOutputCol(indexedFeatures)
                .setMaxCategories(4)
                .fit(output);


        GBTClassifier gbt = new GBTClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures")
                .setMaxIter(10);

        // Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol(predict)
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {labelIndexer, featureIndexer, gbt, labelConverter});

        PipelineModel model = pipeline.fit(trainingData);

        // Make predictions.
        DataFrame predictions = model.transform(testData);

        // Select example rows to display.
        predictions.select("demnda","predictedLabel", "features").show(50);

        // Select (prediction, true label) and compute test error
        MulticlassClassificationEvaluator evaluator_accuracy = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");

        double accuracy = evaluator_accuracy.evaluate(predictions);
        System.out.println("Accuracy = " + (1.0 - accuracy));

        MulticlassClassificationEvaluator evaluator_recall = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("recall");

        double recall = evaluator_recall.evaluate(predictions);

        MulticlassClassificationEvaluator evaluator_precision = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("precision");

        double precision = evaluator_precision.evaluate(predictions);



        RandomForestClassificationModel rfModel = (RandomForestClassificationModel)(model.stages()[2]);
        System.out.println("Learned classification forest model:\n" + rfModel.toDebugString());
        return accuracy;
    }

    public DataFrame getFeaturesFrame(DataFrame df,String[] featureCols){

        return new MLDataParser().getFeaturesFrame(df,featureCols, generated_feature_col_name);
    }
}
