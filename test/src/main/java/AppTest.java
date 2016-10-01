import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.inquisitors.framework.common_utility.Beans.CrimeDataBeanWithTime;
import org.inquisitors.framework.common_utility.Beans.CrimeWithGrid;
import org.inquisitors.framework.common_utility.Config;
import org.inquisitors.framework.common_utility.CrimeUtil;
import org.inquisitors.framework.front_connector.application.DataReader;
import org.inquisitors.framework.ml.Beans.TrainData;
import org.inquisitors.framework.ml.impl.PerceptionClassifier;
import org.inquisitors.framework.ml.impl.RandomForestCrimeClassifierImpl;
import org.inquisitors.framework.ml.util.MLDataParser;
import scala.Tuple2;
import java.io.Serializable;

/**
 * Created by Thilina on 8/26/2016.
 */
public class AppTest implements Serializable{

    public static void main(String args[]){
        AppTest statAnlyzerTest=new AppTest();
        statAnlyzerTest.Test();
    }

    public void Test(){

        Config.configure();
        DataReader dr=new DataReader();
        DataFrame df=dr.readCSV("D:/ML/train.csv", 1);
        ml(df);

    }

    private void ml(DataFrame df){
        try {
            df.show(20);
            DataFrame dataFrame=df;
            CrimeUtil cu=new CrimeUtil();

            dataFrame=cu.getTimeIndexedDF(df, "Dates");

            DataFrame myDataframe = new MLDataParser().indexColumn(dataFrame, "category", "categoryIndex");
            myDataframe.show(20);

            /* remove low frequnt categories */
            JavaRDD<CrimeDataBeanWithTime> crimeDataBeanJavaRDD = myDataframe.javaRDD().map(new Function<Row, CrimeDataBeanWithTime>() {
                public CrimeDataBeanWithTime call(Row row) {

                    CrimeDataBeanWithTime crimeDataBean=null;

                    if(row.getDouble(7)< 10){
                        crimeDataBean = new CrimeDataBeanWithTime(row.getInt(4),row.getString(0),row.getString(1),row.getString(2),row.getString(3),(row.getDouble(5)*(1000) +122000 +500)/100,(row.getDouble(6)*1000 -30000-7700)/100);
                    }
                    else{
                        crimeDataBean = new CrimeDataBeanWithTime(row.getInt(4),"OTHER OFFENSES",row.getString(1),row.getString(2),row.getString(3),(row.getDouble(5)*(1000) +122000 +500)/100,(row.getDouble(6)*1000 -30000-7700)/100);
                    }
                    return crimeDataBean;
                }
            });

            dataFrame=Config.getSqlContext().createDataFrame(crimeDataBeanJavaRDD, CrimeDataBeanWithTime.class);


            /**/
            myDataframe = new MLDataParser().indexColumn(dataFrame, "category", "categoryIndex");
            myDataframe.show(500);


            String[] featureCol = {"timeIndex", "dayOfWeek", "pdDistrict","resolution","x","y"};
            String label = "categoryIndex";

            Tuple2<String, String>[] f = myDataframe.dtypes();
            TrainData trainData = new TrainData(myDataframe, featureCol, label);

            int[] layers = new int[]{featureCol.length,500,39};

            PerceptionClassifier classifier = new PerceptionClassifier();
            classifier.setIsStanderdize(true);
            MulticlassMetrics metrics = classifier.evaluateModel(trainData, 0.8, layers, 256, 1234L, 100);
            System.out.println("==================================================");
            System.out.println(metrics);
            System.out.println("==================================================");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    private void randomForest(DataFrame df){
        try {

            DataFrame dataFrame=df;
            CrimeUtil cu=new CrimeUtil();

            dataFrame=cu.getTimeIndexedDF(df, "Dates");
            dataFrame.show(20);

           /* */


            DataFrame myDataframe = new MLDataParser().indexColumn(dataFrame, "category", "categoryIndex");
            myDataframe.show(20);

            /* remove low frequnt categories */
            JavaRDD<CrimeDataBeanWithTime> crimeDataBeanJavaRDD = myDataframe.javaRDD().map(new Function<Row, CrimeDataBeanWithTime>() {
                public CrimeDataBeanWithTime call(Row row) {

                    CrimeDataBeanWithTime crimeDataBean=null;

                    if(row.getDouble(7)< 10){
                        crimeDataBean = new CrimeDataBeanWithTime(row.getInt(4),row.getString(0),row.getString(1),row.getString(2),row.getString(3),(row.getDouble(5)*(1000) +122000 +500)/100,(row.getDouble(6)*1000 -30000-7700)/100);
                    }
                    else{
                        crimeDataBean = new CrimeDataBeanWithTime(row.getInt(4),"OTHER OFFENSES",row.getString(1),row.getString(2),row.getString(3),(row.getDouble(5)*(1000) +122000 +500)/100,(row.getDouble(6)*1000 -30000-7700)/100);
                    }
                    return crimeDataBean;
                }
            });

            dataFrame=Config.getSqlContext().createDataFrame(crimeDataBeanJavaRDD, CrimeDataBeanWithTime.class);


            /**/
            myDataframe = new MLDataParser().indexColumn(dataFrame, "category", "categoryIndex");
            myDataframe.show(500);


            String[] featureCol = {"timeIndex", "dayOfWeek", "pdDistrict","resolution","x","y"};
            String label = "categoryIndex";

            Tuple2<String, String>[] f = myDataframe.dtypes();
            TrainData trainData = new TrainData(myDataframe, featureCol, label);


            RandomForestCrimeClassifierImpl classifier= new RandomForestCrimeClassifierImpl();
           // classifier.setIsStanderdize(true);
            MulticlassMetrics metrics = classifier.evaluateModel(trainData, 0.8);
            System.out.println("==================================================");
            System.out.println(metrics);
            System.out.println("==================================================");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    private void randomForestAlt(DataFrame df){
        try {

            DataFrame dataFrame=df;
            CrimeUtil cu=new CrimeUtil();

            dataFrame=cu.getTimeIndexedDF(df, "Dates");
            dataFrame.show(20);

            DataFrame myDataframe = new MLDataParser().indexColumn(dataFrame, "category", "categoryIndex");
            myDataframe.show(20);

            /* remove low frequnt categories */
            JavaRDD<CrimeDataBeanWithTime> crimeDataBeanJavaRDD = myDataframe.javaRDD().map(new Function<Row, CrimeDataBeanWithTime>() {
                public CrimeDataBeanWithTime call(Row row) {

                    CrimeDataBeanWithTime crimeDataBean=null;
                    if(row.getDouble(9)< 10){
                        crimeDataBean = new CrimeDataBeanWithTime(row.getInt(4),row.getString(0),row.getString(1),row.getString(2),row.getString(3),row.getDouble(5),row.getDouble(6));
                    }
                    else{
                        crimeDataBean = new CrimeDataBeanWithTime(row.getInt(4),"OTHER OFFENSES",row.getString(1),row.getString(2),row.getString(3),row.getDouble(5),row.getDouble(6));
                    }
                    return crimeDataBean;
                }
            });

            DataFrame dataFrame1=Config.getSqlContext().createDataFrame(crimeDataBeanJavaRDD, CrimeDataBeanWithTime.class);
            dataFrame1.show(30);

            /**/
            myDataframe = new MLDataParser().indexColumn(dataFrame1, "category", "categoryIndex");
            myDataframe.show(500);

            String[] featureCol = {"timeIndex", "dayOfWeek", "pdDistrict","resolution","x","y"};
            String label = "categoryIndex";

            Tuple2<String, String>[] f = myDataframe.dtypes();
            TrainData trainData = new TrainData(myDataframe, featureCol, label);


            RandomForestCrimeClassifierImpl classifier= new RandomForestCrimeClassifierImpl();
            // classifier.setIsStanderdize(true);
            MulticlassMetrics precision = classifier.evaluateModel(trainData, 0.8);
            System.out.println("==================================================");
            System.out.println(precision);
            System.out.println("==================================================");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    private void cpyml(DataFrame df){
        try {
            df.show(20);
            DataFrame dataFrame=df;
            CrimeUtil cu=new CrimeUtil();

            dataFrame=cu.getTimeIndexedDF(df, "Dates");

            /* */
            MLDataParser mlDataParser=new MLDataParser();

            dataFrame=mlDataParser.discritizeColoumn(dataFrame,"x",100,"indX");
            dataFrame=mlDataParser.discritizeColoumn(dataFrame,"y",100,"indY");


            DataFrame myDataframe = new MLDataParser().indexColumn(dataFrame, "category", "categoryIndex");
            myDataframe.show(20);

            /* remove low frequnt categories */
            JavaRDD<CrimeWithGrid> crimeDataBeanJavaRDD = myDataframe.javaRDD().map(new Function<Row, CrimeWithGrid>() {
                public CrimeWithGrid call(Row row) {

                    CrimeWithGrid crimeDataBean=null;
                    double grid=(row.getDouble(7)*10 + row.getDouble(8))/4;
                    if(row.getDouble(7)< 6){
                        crimeDataBean = new CrimeWithGrid(row.getInt(4),row.getString(0),row.getString(1),row.getString(2),row.getString(3),grid);
                    }
                    else{
                        crimeDataBean = new CrimeWithGrid(row.getInt(4),"OTHER OFFENSES",row.getString(1),row.getString(2),row.getString(3),grid);
                    }
                    return crimeDataBean;
                }
            });

            DataFrame dataFrame1=Config.getSqlContext().createDataFrame(crimeDataBeanJavaRDD, CrimeWithGrid.class);

            /**/
            myDataframe = new MLDataParser().indexColumn(dataFrame1, "category", "categoryIndex");
            myDataframe.show(500);

            String[] featureCol = {"timeIndex", "dayOfWeek", "pdDistrict","resolution","grid"};
            String label = "categoryIndex";

            Tuple2<String, String>[] f = myDataframe.dtypes();
            TrainData trainData = new TrainData(myDataframe, featureCol, label);

            int[] layers = new int[]{featureCol.length,50,39};

            PerceptionClassifier classifier = new PerceptionClassifier();
            classifier.setIsStanderdize(true);
            MulticlassMetrics precision = classifier.evaluateModel(trainData, 0.8, layers, 256, 1234L, 100);
            System.out.println("==================================================");
            System.out.println(precision);
            System.out.println("==================================================");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }


/*
    public void frontCOn(DataFrame df){
        StatAnalyzer stat_analyzer=new StatAnalyzer();

        stat_analyzer.timeWiseData(df,0,5,"category");
        //String[] cols=new String[]{"WARRANTS","ASSAULT"};
        //stat_analyzer.categoryWiseData(df,"ASSAULT","dayOfWeek");

    }*/
    /* private void categoryWiseData(DataFrame df){
        StatisticalAnalyzerCore statisticalAnalyzerCore =new StatisticalAnalyzerCore();
        String[] cols=new String[]{"WARRANTS","ASSAULT"};
        DataFrame frame= statisticalAnalyzerCore.categoryWiseData(df,cols);
        frame.show(30);

        DataSummary st= statisticalAnalyzerCore.getSummary(df, "Category", "ASSAULT");
        System.out.println(st.toString());

        //time wise data
        StatAnalyzer statAnalyzer=new StatAnalyzer();
        statAnalyzer.timeWiseData(df,0,5,"category");
    }*/

   /* private void FPGrowth(DataFrame df){
//stat summry
        String[] cols=new String[]{"Category","DayOfWeek", "PdDistrict"};
        Stat_Summary stat=new Stat_SummaryImpl();
        CrimeUtil cu=new CrimeUtil();

        df=cu.getTimeIndexedDF(df, "Dates");
        //fp
        List<frequentCrimePatterns> list=stat.mineFrequentPatterns(df,0.02, 0.05);
        System.out.println("------------------------");
        System.out.println(list);
        List<frequentCrimeSet> list2=stat.frequentItemSets(df, 0.05);

        list2.forEach(items -> System.out.println(items.getFrequentCrimePatterns().toString() + "  " + items.getFrequency()));


    }*/

}
