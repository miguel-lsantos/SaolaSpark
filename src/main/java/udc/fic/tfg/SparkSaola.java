package udc.fic.tfg;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import java.util.*;
import org.apache.spark.sql.SparkSession;

public class SparkSaola {

        private static SparkSession spark;
        private static JavaSparkContext sparkContext;

    public static void main(String[] args) {
        if (args.length != 5) {
            System.out.println("5 Argumentos Necesarios: file, rows, columns, threshold, multiplier");
            return;
        }
        String file = args[0];
        int rows = Integer.parseInt(args[1]);
        int columns = Integer.parseInt(args[2]);
        double threshold = Double.parseDouble(args[3]);
        int multiplier = Integer.parseInt(args[4]);

        SparkConf conf = new SparkConf();
        conf.set("spark.driver.maxResultSize", "10g");

        spark = SparkSession.builder().config(conf).getOrCreate();

        sparkContext = new JavaSparkContext(spark.sparkContext());

        //sparkContext.setLogLevel("ERROR");

        Date date = new Date();
        long startTime = date.getTime();
        //int classFeatureIndex = 0;

        JavaRDD<String> stringRDD = sparkContext.textFile(file, sparkContext.defaultParallelism() * multiplier);
        Broadcast<Integer> columnsBroadcast = sparkContext.broadcast(columns);
        JavaPairRDD<Integer, Double> byColumn = stringRDD.flatMapToPair((line) -> {
            List<Tuple2<Integer, Double>> columnAndRowList = new ArrayList<>(columnsBroadcast.getValue());
            String[] wordArray = line.split(",");
            for(int j = 0; j<wordArray.length; j++){
                columnAndRowList.add(Tuple2.apply(j, Double.parseDouble(wordArray[j])));
            }
            return columnAndRowList.iterator();
//            int column = 0;
//            int i = 0;
//            int j = line.indexOf(',');
//            while (j >= 0)
//            {
//                columnAndRowList.add(new Tuple2<>(column++, Double.parseDouble(line.substring(i, j))));
//                i = j + 1;
//                j = line.indexOf(',', i);
//            }
//            columnAndRowList.add(new Tuple2<>(column, Double.parseDouble(line.substring(i))));
//
//            return columnAndRowList.iterator();
        });

//        JavaPairRDD<Integer, List<Double>> byColumn2 = byColumn.combineByKey(((value) -> new ArrayList<Double>() {{ add(value); }}),
//        ((acc, value) -> {
//            acc.add(value);
//            return acc;
//        }), ((acc1, acc2) -> {
//            acc1.addAll(acc2);
//            return acc1;
//        }), sparkContext.defaultParallelism() * 3);
        JavaPairRDD<Integer, ArrayList<Double>> byColumn2 = byColumn.aggregateByKey(new ArrayList<>(),
                new SeqPartitioner(columns,sparkContext.defaultParallelism() * multiplier),
                ((acc, value) -> {
                    acc.add(value);
                    return acc;
                }), ((acc1, acc2) -> {
                    acc1.addAll(acc2);
                    return acc1;
                }));

        Broadcast<Double> doubleRowsBroadcast = sparkContext.broadcast((double) rows);
//        byColumn.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Double>>, Tuple2<Integer, Double>>() {
//            @Override
//            public Iterator<Tuple2<Integer, Double>> call(Iterator<Tuple2<Integer, Double>> tuple2Iterator) throws Exception {
//                List<Tuple2<Integer, Double>> actualList = new ArrayList<>();
//                tuple2Iterator.forEachRemaining(actualList::add);
//                actualList.sort((x,y) -> { return x._1().compareTo(y._1());});
//                return actualList.iterator();
//            }
//        }, true);

//        byColumn = byColumn.foldByKey("", ((v1, v2) -> {return v1 + "," + v2;}));


//        Feature classFeature = byColumn2.filter((x) -> x._1()==0).map((tuple) -> {
//            int numberRows = doubleRowsBroadcast.getValue().intValue();
//            short[] featureArray = new short[numberRows];
//            List<Double> wordList = tuple._2();
//            short number;
//            int maxNumber = 0;
//            int minNumber = 0;
//
//            for(int j = 0; j<wordList.size(); j++){
//                double numberDouble = wordList.get(j);
//                number = (short) Math.floor(numberDouble);
//                featureArray[j] = number;
//                if (number > maxNumber)
//                    maxNumber = number;
//                if (number < minNumber)
//                    minNumber = number;
//            }
//
//            Feature feature = new Feature(featureArray, tuple._1, maxNumber-minNumber+1, minNumber);
//
//            feature.calculateEntropy(numberRows);
//
//            return feature;
//        }).first();

        JavaRDD<Feature> featureJavaRDD = byColumn2.map((tuple) -> {
            short[] featureArray = new short[doubleRowsBroadcast.getValue().intValue()];
            List<Double> wordList = tuple._2();
            short number;
            int maxNumber = 0;
            int minNumber = 0;
            double sum = 0;

            for(int j = 0; j<wordList.size(); j++){
                double numberDouble = wordList.get(j);
                sum += numberDouble;
                number = (short) Math.floor(numberDouble);
                featureArray[j] = number;
                if (number > maxNumber)
                    maxNumber = number;
                if (number < minNumber)
                    minNumber = number;
            }

            if (sum == 0 && tuple._1 > 0){
                return new Feature();
            }

            return new Feature(featureArray, tuple._1, maxNumber-minNumber+1, minNumber);
        }).cache();

        Feature classFeature = featureJavaRDD.filter((feature) -> feature.getIndex()==0).map((feature) -> {
            feature.calculateEntropy(doubleRowsBroadcast.getValue());
            return feature;
        }).first();

        Broadcast<Feature> classFeatureBroadcast = sparkContext.broadcast(classFeature);
        Broadcast<Double> thresholdBroadcast = sparkContext.broadcast(threshold);

        featureJavaRDD = featureJavaRDD.map((feature) -> {
            feature.classSU(classFeatureBroadcast.getValue(), doubleRowsBroadcast.getValue());
            return feature;
        }).filter((feature) -> (feature.getClassSU() > thresholdBroadcast.getValue()) && (feature.getIndex()>0));

        Function2<List<Feature>, Feature, List<Feature>> seqOp2 = (acc, v) -> Feature.addImportantFeatures(acc, v, doubleRowsBroadcast.getValue(), thresholdBroadcast.getValue());

        Function2<List<Feature>, List<Feature>, List<Feature>> combOp2 = (v1, v2) -> Feature.addImportantFeatures(v1, v2, doubleRowsBroadcast.getValue(), thresholdBroadcast.getValue());

        List<Feature> featureList = new ArrayList<>();
        featureList = featureJavaRDD.aggregate(featureList, seqOp2, combOp2);
        System.out.print("Feature List : ");
        System.out.println(featureList.size());
        for(Feature feature : featureList){
            System.out.println(feature.getIndex());
        }

        long time = new Date().getTime() - startTime;
        System.out.print("Time=");
        System.out.println(time);

//        try {
//            Scanner scanner = new Scanner(System.in);
//            scanner.nextLine();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        spark.stop();
    }
}
