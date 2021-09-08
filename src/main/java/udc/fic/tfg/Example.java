package udc.fic.tfg;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Array;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.columnar.ARRAY;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Seq;

public class Example {

	public static class Pair<T, U> {
		public final T t;
		public final U u;

		public Pair(T t, U u) {
			this.t = t;
			this.u = u;
		}
	}

	private static SparkSession spark;
	private static JavaSparkContext sparkContext;
	private static SparkContext sc;
	private static final StructType resultSchema = DataTypes
			.createStructType(new StructField[] { DataTypes.createStructField("result", DataTypes.DoubleType, true), });


	private static final Function2<Double, Double, Double> combine = new Function2<Double, Double, Double>() {
		@Override
		public Double call(Double v1, Double v2) throws Exception {
			return v1 + v2;
		}
	};

	public static void main2(String[] args) {

		spark = SparkSession.builder().master("local[*]").getOrCreate();

		sparkContext = new JavaSparkContext(spark.sparkContext());
		sc = sparkContext.sc();
		StructField[] fields = new StructField[2000];
		for (int i = 0; i < fields.length; i++) {
			fields[i] = DataTypes.createStructField(String.format("_c%d", i), DataTypes.IntegerType, true);
		}

		StructType main_schema = DataTypes.createStructType(fields);
		// sparkContext.setLogLevel("ERROR");

		Dataset<Row> df = spark.read().schema(main_schema)/** .option("mode", "DROPMALFORMED") */
				.csv("C:\\Users\\Miguel\\Desktop\\TFG\\GroupSaloaSpark\\code\\tfg\\src\\main\\resources\\epsilon_normalized.csv")
				.limit(10).cache();
		df.show(10);
		Date date = new Date();
		long startTime = date.getTime();

		int rows = (int) df.count();
		Double double_rows = Double.valueOf(rows);
		System.out.print("Rows=");
		System.out.println(rows);
		long time = new Date().getTime() - startTime;
		// df.show(20);
		System.out.print("Count Time=");
		System.out.println(time);

		String[] all_columns = df.columns();
		// Map<String, String> columns_exprs = new HashMap<String, String>();
		// for(String column : all_columns) {
		// columns_exprs.put(column, "min");
		// }

		// Dataset<Row> min_df = df.agg(columns_exprs);

		// String[] min_columns = min_df.columns();
		// Row values = min_df.first();

		// for(int i = 0; i<min_columns.length; i++) {
		// String column = min_columns[i];
		// column = column.substring(4, column.length()-1);
		// df = df.withColumn(column, functions.col(column).$minus(values.get(i)));
		// }
		// time = new Date().getTime() - startTime;
		// System.out.print("Normalization Time=");
		// System.out.println(time);

		// TODO: create empty df_count
		Dataset<Row> mi_results = spark.createDataFrame(new ArrayList<Row>(), resultSchema);
		for (int i = 0; i < all_columns.length - 1; i++) {
			// TODO: create empty df_count

			Dataset<Row> df_count = df.groupBy(all_columns[i], all_columns[all_columns.length - 1])
					.agg(functions.count(all_columns[1]).$div(double_rows).as("count"));
			// df_count.repartition(functions.col(all_columns[i]));
			// df_count.repartition(functions.col(all_columns[all_columns.length-1]));

			// Dataset<Row> df_count_col_0 =
			// df_count.groupBy(all_columns[i]).agg(functions.sum("count").as("count0"));
			// Dataset<Row> df_count_col_1 =
			// df_count.groupBy(all_columns[all_columns.length-1]).agg(functions.sum("count").as("count1"));
			// df_count = df_count.join(df_count_col_0,
			// df_count.col(all_columns[i]).equalTo(df_count_col_0.col(all_columns[i])),
			// "inner")
			// .join(df_count_col_1,
			// df_count.col(all_columns[all_columns.length-1]).equalTo(df_count_col_1.col(all_columns[all_columns.length-1])),
			// "inner");;

			WindowSpec window_0 = Window.partitionBy(all_columns[i]);
			WindowSpec window_1 = Window.partitionBy(all_columns[all_columns.length - 1]);
			df_count = df_count.select(functions.col("count"),
					functions.regexp_replace(window_0.withAggregate(functions.sum("count")), "", "").as("count0"),
					functions.regexp_replace(window_1.withAggregate(functions.sum("count")), "", "").as("count1"));
			// df_count.show();
			// df_count = df_count.withColumn("count0",
			// window_0.withAggregate(functions.sum("count"))).withColumn("count1",
			// window_1.withAggregate(functions.sum("count")));
			mi_results = mi_results
					.union(df_count
							.agg(functions
									.sum(functions.log(functions.col("count").$div(functions.col("count0"))
											.$div(functions.col("count1"))).$times(functions.col("count")))
									.$div(Math.log(2)))
							.as("result"));
			// Double value1 = df_count.collectAsList().get(0).getDouble(0);

		}
		// df_count.show();

		// Double value1 = df_count.collectAsList().get(0).getDouble(0);
		// System.out.println(value1);

		mi_results.show();

		time = new Date().getTime() - startTime;
		System.out.print("MU Time=");
		System.out.println(time);

		Map<String, Double> column_entropy = new HashMap<String, Double>();
		long startTime2 = new Date().getTime();

		// StructType schema = DataTypes.createStructType(new StructField[] {
		// DataTypes.createStructField("result", DataTypes.DoubleType, true),
		// });
		Dataset<Row> h_results = spark.createDataFrame(new ArrayList<Row>(), resultSchema);

		for (String column : all_columns) {
			String column_result = column + "_result";

			Dataset<Row> df_column = df.groupBy(column)
					.agg(functions.count(column).$div(double_rows).as(column_result));
			h_results = h_results.union(df_column.agg(
					functions.sum(functions.negate(functions.log(column_result).$times(functions.col(column_result))))
							.$div(Math.log(2)).as("result")));
			// h_results.withColumn(column, functions.first(df_column.col(column_result),
			// null));
			// df_column.show();
			// Double value = df_column.collectAsList().get(0).getDouble(0);
			// column_entropy.put(column, value);
			// System.out.println(value);
		}
		h_results.show();

		time = new Date().getTime() - startTime;
		System.out.print("H Time=");
		System.out.println(time);
		time = new Date().getTime() - startTime2;
		System.out.print("H2 Time=");
		System.out.println(time);

		System.out.println("Finish:");
		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		spark.stop();
	}
/*
	public static void main(String[] args) {
		spark = SparkSession.builder().master("local[*]").getOrCreate();

		sparkContext = new JavaSparkContext(spark.sparkContext());
		//sparkContext.setLogLevel("ERROR");
		sc = sparkContext.sc();

		Date date = new Date();
		long startTime = date.getTime();

		double threshold = 0.0;
		int classFeatureIndex = 0;
		int columns = 124;
		int rows = 100000;
		Double doubleRows = Double.valueOf(rows);
		JavaRDD<String> stringRDD = sc.textFile("C:\\Users\\Miguel\\Desktop\\TFG\\GroupSaloaSpark\\code\\tfg\\src\\main\\resources\\epsilon_normalized_100000.csv", 8).toJavaRDD();

		JavaPairRDD<Integer, Tuple2<Integer, Integer>> byColumnAndRow =stringRDD.zipWithUniqueId().flatMapToPair((tuple) -> {
			List<Tuple2<Integer, Tuple2<Integer, Integer>>> columnAndRowList = new ArrayList<>();
			String[] wordArray = tuple._1.split(",");
			for(int i = 0; i<wordArray.length; i++){
				columnAndRowList.add(Tuple2.apply(i, Tuple2.apply(tuple._2.intValue(), (int) Math.floor(Double.parseDouble(wordArray[i])))));
			}
			return columnAndRowList.iterator();
		});

		Broadcast<Double> doubleRowsBroadcast = sparkContext.broadcast(doubleRows);

		JavaRDD<Feature> featureJavaRDD = byColumnAndRow.groupByKey().map((tuple) -> {
			short[] featureArray = new short[doubleRowsBroadcast.getValue().intValue()];
			Iterator<Tuple2<Integer, Integer>> iterator = tuple._2.iterator();
			int number;
			int maxNumber = 0;
			int minNumber = 0;
			int i = 0;
			while (iterator.hasNext()) {
				number = iterator.next()._2();
				featureArray[i] = (short) number;
				if (number > maxNumber)
					maxNumber = number;
				if (number < minNumber)
					minNumber = number;
				i++;
			}

			return new Feature(featureArray, tuple._1, maxNumber-minNumber+1, minNumber);
		});
		featureJavaRDD.cache();

		Feature classFeature  = featureJavaRDD.filter((feature) -> feature.getIndex() == 0).first();
		featureJavaRDD = featureJavaRDD.filter((feature) -> feature.getIndex() != 0);

		classFeature.calculateEntropy(doubleRows);
		Broadcast<Feature> classFeatureBroadcast = sparkContext.broadcast(classFeature);

		Broadcast<Double> thresholdBroadcast = sparkContext.broadcast(threshold);

		Function2<List<Feature>, Feature, List<Feature>> seqOp = new Function2<List<Feature>, Feature, List<Feature>>() {

			@Override
			public List<Feature> call(List<Feature> acc, Feature v) {
				return Feature.addImportantFeatures(acc, v, classFeatureBroadcast.getValue(), doubleRowsBroadcast.getValue(), thresholdBroadcast.getValue());
			}
		};

		Function2<List<Feature>, List<Feature>, List<Feature>> combOp = new Function2<List<Feature>, List<Feature>, List<Feature>>() {

			@Override
			public List<Feature> call(List<Feature> v1, List<Feature> v2) {
				return Feature.addImportantFeatures(v1, v2, doubleRowsBroadcast.getValue(), thresholdBroadcast.getValue());
			}
		};
		List<Feature> featureList = new ArrayList<>();
		featureList = featureJavaRDD.aggregate(featureList, seqOp, combOp);
		System.out.print("Feature List : ");
		System.out.println(featureList.size());
		for(Feature feature : featureList){
			System.out.println(feature.getIndex());
		}

		long time = new Date().getTime() - startTime;
		System.out.print("Time=");
		System.out.println(time);

		featureJavaRDD.unpersist();

		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		spark.stop();
	}*/

	public static void main4(String[] args) {

		spark = SparkSession.builder().master("local[*]").getOrCreate();

		sparkContext = new JavaSparkContext(spark.sparkContext());
		sc = sparkContext.sc();
		StructField[] fields = new StructField[785];
		for (int i = 0; i < fields.length; i++) {
			fields[i] = DataTypes.createStructField(String.format("_c%d", i), DataTypes.createDecimalType(5, 0), true);
		}

		StructType mainSchema = DataTypes.createStructType(fields);
		//sparkContext.setLogLevel("ERROR");

		Dataset<Row> df = spark.read().schema(mainSchema)/** .option("inferSchema", "true") */
				.csv("C:\\Users\\Miguel\\Desktop\\TFG\\GroupSaloaSpark\\code\\tfg\\src\\main\\resources\\mnist8m.csv")
				.limit(2000000).cache();


		//Datasetdf.map()
		// Variables
		double threshold = 0;
		int classAttribute = 0;
		
		Date date = new Date();
		long startTime = date.getTime();

		//int rows = (int) df.count();
		int rows = 2000000;
		Double doubleRows = Double.valueOf(rows);

		System.out.print("Rows=");
		System.out.println(rows);


        //ReduceFunction<Double> hFunc = (accum, n) -> (accum - (Math.log(n)*n));
        Function2<Double, Double, Double> hFunc = new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double a, Double x) {
                double division = x / doubleRows;
                a -= division * Math.log(division);
                return a;
            }
        };
        Double initialValue = 0.0;

        Dataset<Row> finalDf = df;
        Function2<List<Double>, StructField,List<Double>> prueba = new Function2<List<Double>, StructField,List<Double>>() {
            @Override
            public List<Double> call(List<Double> v1, StructField v2) throws Exception {
                Dataset<Row> dfColumn = finalDf.groupBy(v2.name()).count();
                double entropy = dfColumn.select("count").as(Encoders.DOUBLE()).javaRDD().aggregate(initialValue, hFunc, combine) / Math.log(2);
                v1.add(entropy);
                return v1;
            }
        };
        List<Double> initialValue1 = new ArrayList<Double>();
        //List<Double> result = mainSchema.foldLeft(initialValue1, prueba);

		long time = new Date().getTime() - startTime;
		System.out.print("Count Time=");
		System.out.println(time);
		
		String[] allColumns = df.columns();

		//Dataset<Row> hResults = getAllColumnsEntropy(df, allColumns, doubleRows);
		//List<Row> hResultList = hResults.collectAsList();

		Pair<Map<Integer, Double>, Dataset<Row>> pairH = getAllColumnsEntropy2(df, allColumns, doubleRows);
		Map<Integer, Double> hResultMap = pairH.t;
		df = pairH.u;

		allColumns = df.columns();

		Pair<Map<Integer, Double>, Dataset<Row>> pairM = removeIrrelevantFeatures2(df, allColumns, doubleRows,
				hResultMap, threshold, classAttribute);

		Map<Integer, Double> muResultsMap = pairM.t;

		df = pairM.u;

		df = df.drop(getColumnFromInteger(classAttribute));

		allColumns = df.columns();

		df = removeRedundantFeatures2(df, allColumns, doubleRows, hResultMap, muResultsMap, threshold);
		//df = removeRedundantFeatures(df, allColumns, doubleRows, hResultList, muResultsMap, classAttribute, threshold);

		allColumns = df.columns();

		System.out.println();
		System.out.println(Arrays.toString(allColumns));
		time = new Date().getTime() - startTime;
		System.out.print("Time=");
		System.out.println(time);
		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		spark.stop();
	}




	public static void main3(String[] args) {
		spark = SparkSession.builder().master("local[*]").getOrCreate();

		sparkContext = new JavaSparkContext(spark.sparkContext());
		sc = sparkContext.sc();
		StructField[] fields = new StructField[124];
		for (int i = 0; i < fields.length; i++) {
			fields[i] = DataTypes.createStructField(String.format("_c%d", i), DataTypes.createDecimalType(5, 0), true);
		}

		StructType mainSchema = DataTypes.createStructType(fields);
		sparkContext.setLogLevel("ERROR");

		Dataset<Row> df = spark.read().schema(mainSchema)/** .option("inferSchema", "true") */
				.csv("C:\\Users\\Miguel\\Desktop\\TFG\\GroupSaloaSpark\\code\\tfg\\src\\main\\resources\\a9a.csv")
				.limit(1000000).cache();
		df.groupBy("_c0").count().show();
		// Variables
		double threshold = 0;
		int classAttribute = 0;
		String classAttributeStr = String.format("_c%d", classAttribute);

		Date date = new Date();
		long startTime = date.getTime();

		//int rows = (int) df.count();
		int rows = 1000000;
		Double doubleRows = Double.valueOf(rows);

		System.out.print("Rows=");
		System.out.println(rows);

		long time = new Date().getTime() - startTime;
		System.out.print("Count Time=");
		System.out.println(time);

		String[] allColumns = df.columns();

		Pair<Map<Integer,Double>, Dataset<Row>> pairH = getAllColumnsEntropy2(df, allColumns, doubleRows);
		Map<Integer,Double> hResultMap = pairH.t;
		df = pairH.u;

		allColumns = df.columns();
		double classMu = hResultMap.get(classAttribute);

		double[] dep = new double[allColumns.length];
		ArrayList<Integer> currentFeature = new ArrayList<Integer>();
		for(int j = 0; j<allColumns.length; j++) {
			System.out.print(String.valueOf(j) + ":  ");

			if (j == classAttribute) {
				continue;
			}

			//double n1 = Arrays.stream(matrix.getColumn(featureIndex[j])).sum();
			//if (n1==0)
			//	continue;
			System.out.println(allColumns[j]);
			System.out.println(classAttributeStr);
			System.out.println(hResultMap.get(j));
			System.out.println(classMu);
			double value = getMultualInformation2(df, allColumns[j], classAttributeStr,  doubleRows,  hResultMap.get(j) + classMu);
			dep[j] = value;
			System.out.println(value);
			//REMOVE IRRELEVANT
			if (value <= threshold)
				continue;

			ArrayList<Integer> currentFeature1 = new ArrayList<Integer>(currentFeature);
			currentFeature.add(j);

			int p = currentFeature1.size();
			if (p>0) {

				for(int k = 0; k<p; k++){
					int t_feature = currentFeature1.get(k);
					double dep_ij = getMultualInformation2(df, allColumns[j], allColumns[t_feature],  doubleRows,  hResultMap.get(j) + hResultMap.get(t_feature));

					if (dep_ij<=threshold)
						continue;

					//REMOVE 1ST REDUNDANT
					if (dep[t_feature]>=dep[j] && dep_ij>Math.min(dep[j],dep[t_feature])) {
						currentFeature.remove(p - 1);
						break;
					}

					//REMOVE 2ST REDUNDANT
					if (dep[j]>dep[t_feature] && dep_ij>Math.min(dep[j],dep[t_feature]))
						currentFeature.remove(k);
				}
			}
		}//for j=1:length(f_g_index)
		System.out.println();
		System.out.println(currentFeature.toString());
		time = new Date().getTime() - startTime;
		System.out.print("Time=");
		System.out.println(time);
		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		spark.stop();
	}

	private static Dataset<Row> removeRedundantFeatures2(Dataset<Row> df, String[] allColumns, Double doubleRows, Map<Integer, Double> hResultList, Map<Integer, Double> muResultsMap, double threshold) {
		List<String> drops = new ArrayList<String>();
		for (int i = 0; i < allColumns.length-1; i++) {
			System.out.println(i);
			if (drops.contains(allColumns[i])) {
				continue;
			}
			Integer first = getIntegerFromColumn(allColumns[i]);
			Double firstMu = muResultsMap.get(first);
			for (int j = i + 1; j < allColumns.length; j++) {
				if (drops.contains(allColumns[j])) {
					continue;
				}
				Integer second = getIntegerFromColumn(allColumns[j]);
				Double secondMu = muResultsMap.get(second);
				Double betweenMu = getMultualInformation2(df, allColumns[i], allColumns[j], doubleRows,
						hResultList.get(first) + hResultList.get(second));

				if ( (betweenMu > threshold) && (betweenMu > Math.min(firstMu, secondMu))) {
					if (firstMu >= secondMu) {
						drops.add(getColumnFromInteger(second));
						System.out.print("Removed ");
						System.out.println(second);
					} else {
						drops.add(getColumnFromInteger(first));
						System.out.print("Removed ");
						System.out.println(first);
						break;
					}
				}
			}
		}

		String[] arguments = drops.toArray(new String[drops.size()]);
		return df.drop(arguments);
	}

	private static Dataset<Row> removeRedundantFeatures(Dataset<Row> df, String[] allColumns, Double doubleRows,
														List<Double> hResultList, Map<Integer, Double> muResultsMap, int classAttribute, double threshold) {

		List<String> drops = new ArrayList<String>();
		Map<Integer, List<Integer>> featureCombinations = initFeatureCombinations(allColumns, classAttribute);

		Set<Integer> removedFeatures = new HashSet<Integer>();
		while (!featureCombinations.isEmpty()) {
			Set<Integer> usedFeatures = new HashSet<Integer>();
			
			List<Pair<Integer, Integer>> selectedCombinations = new ArrayList<Pair<Integer, Integer>>();
			for (Integer feature : removedFeatures) {
				featureCombinations.remove(feature);
			}
			
			for (Integer key : featureCombinations.keySet()) {
				if (!usedFeatures.contains(key)) {
					List<Integer> values = featureCombinations.get(key);
					values.removeAll(removedFeatures);
					for (int i = 0; i < values.size(); i++) {
						Integer value = values.get(i);
						if (!usedFeatures.contains(value)) {
							usedFeatures.add(value);
							values.remove(i);
							selectedCombinations.add(new Pair<Integer, Integer>(key, value));
							System.out.print(key);
							System.out.print(" : ");
							System.out.println(value);
							break;
						}
					}
					if (values.isEmpty()){
						removedFeatures.add(key);
					}
				}
			}

			Dataset<Double> muResults = spark.createDataset(new ArrayList<Double>(), Encoders.DOUBLE());
			for (Pair<Integer, Integer> combination : selectedCombinations) {
				muResults = muResults.union(getMultualInformation(df, getColumnFromInteger(combination.t),
						getColumnFromInteger(combination.u), doubleRows,
						hResultList.get(combination.t) + hResultList.get(combination.u)));
			}
			List<Double> muResultsList = muResults.collectAsList();

			int i = 0;
			for (Pair<Integer, Integer> combination : selectedCombinations) {
				Object posibleNull = muResultsList.get(i);
				double value;

				if (posibleNull != null) {
					value = (double) posibleNull;
				} else {
					System.out.println("0/0");
					continue;
				}

				int first = combination.t;
				int second = combination.u;
				double firstMu = muResultsMap.get(first);
				double secondMu = muResultsMap.get(second);
				if ((value > threshold) && (value > Math.min(firstMu, secondMu))) {
					if (firstMu >= secondMu) {
						drops.add(getColumnFromInteger(second));
						removedFeatures.add(second);
						System.out.print("Removed ");
						System.out.println(second);
					} else {
						drops.add(getColumnFromInteger(first));
						removedFeatures.add(first);
						System.out.print("Removed ");
						System.out.println(first);
					}
				}
				i++;
			}
		}
		String[] arguments = drops.toArray(new String[drops.size()]);
		return df.drop(arguments);
	}

	private static Map<Integer, List<Integer>> initFeatureCombinations(String[] allColumns, int classAttribute) {
		Map<Integer, List<Integer>> featureCombinations = new HashMap<>();
		for (int i = 0; i < allColumns.length - 1; i++) {
			Integer key = getIntegerFromColumn(allColumns[i]);
			if (key != classAttribute) {
				List<Integer> combination = new ArrayList<Integer>();
				for (int j = i + 1; j < allColumns.length; j++) {
					Integer value = getIntegerFromColumn(allColumns[j]);
					if (value != classAttribute) {
						combination.add(getIntegerFromColumn(allColumns[j]));
						System.out.print(getIntegerFromColumn(allColumns[j]));
						System.out.print(" ");
					}
				}
				featureCombinations.put(key, combination);
				System.out.print(" : ");
				System.out.println(key);
			}
		}

		return featureCombinations;
	}

	private static Integer getIntegerFromColumn(String column) {
		return Integer.valueOf(column.substring(2));
	}

	private static String getColumnFromInteger(Integer column) {
		return String.format("_c%d", column);
	}
	
	private static Pair<Map<Integer, Double>, Dataset<Row>> removeIrrelevantFeatures2(Dataset<Row> df,
																					  String[] allColumns, double doubleRows, Map<Integer, Double> hResultList, double threshold, int classAttribute) {

		Dataset<Row> muResults = spark.createDataFrame(new ArrayList<Row>(), resultSchema);
		// Dataset<Row> hResults2 = spark.createDataFrame(new ArrayList<Row>(), schema);
		List<Double> muResultsList = new ArrayList<Double>();
		for (int i = 0; i < allColumns.length; i++) {
			if (i != classAttribute) {
				muResultsList.add(getMultualInformation2(df, allColumns[i], getColumnFromInteger(classAttribute),
					doubleRows, hResultList.get(i) + hResultList.get(classAttribute)));
			}
		}
		// hResults2.show();
		//List<Row> muResultsList = muResults.collectAsList();
		List<String> drops = new ArrayList<>();
		Map<Integer, Double> muResultsMap = new HashMap<>();

		for (int i = 0; i < muResultsList.size(); i++) {
			Object posibleNull = muResultsList.get(i);
			double value = -1;

			if (posibleNull != null) {
				value = (double) posibleNull;
			}
			if (value <= threshold) {
				if (i < classAttribute) {
					drops.add(String.format("_c%d", i));
				} else {
					drops.add(String.format("_c%d", i + 1));
				}
			} else {
				if (i < classAttribute) {
					muResultsMap.put(i, value);
				} else {
					muResultsMap.put(i + 1, value);
				}
			}
		}
		String[] arguments = drops.toArray(new String[drops.size()]);
		System.out.println(Arrays.toString(arguments));
		Pair<Map<Integer, Double>, Dataset<Row>> pair = new Pair<>(muResultsMap,
				df.drop(arguments));
		return pair;
	}

	private static Pair<Map<Integer, Double>, Dataset<Row>> removeIrrelevantFeatures(Dataset<Row> df,
			String[] allColumns, double doubleRows, List<Double> hResultList, double threshold, int classAttribute) {

		Dataset<Double> muResults = spark.createDataset(new ArrayList<>(), Encoders.DOUBLE());
		// Dataset<Row> hResults2 = spark.createDataFrame(new ArrayList<Row>(), schema);
		for (int i = 0; i < allColumns.length; i++) {
			if (i != classAttribute) {
				Dataset<Double> mu = getMultualInformation(df, allColumns[i], allColumns[classAttribute],
						doubleRows, hResultList.get(i) + hResultList.get(classAttribute)).as(Encoders.DOUBLE());
				muResults = muResults.union(mu);
				//if (i % 5 == 4) {
				//	muResults.collect();
				//}
			}
		}
		// hResults2.show();
		List<Double> muResultsList = muResults.as(Encoders.DOUBLE()).collectAsList();
		List<String> drops = new ArrayList<>();
		Map<Integer, Double> muResultsMap = new HashMap<>();

		for (int i = 0; i < muResultsList.size(); i++) {
			Object posibleNull = muResultsList.get(i);
			double value = -1;

			if (posibleNull != null) {
				value = (double) posibleNull;
			}
			if (value <= threshold) {
				if (i < classAttribute) {
					drops.add(String.format("_c%d", i));
				} else {
					drops.add(String.format("_c%d", i + 1));
				}
			} else {
				if (i < classAttribute) {
					muResultsMap.put(i, value);
				} else {
					muResultsMap.put(i + 1, value);
				}
			}
		}
		String[] arguments = drops.toArray(new String[drops.size()]);
		System.out.println(Arrays.toString(arguments));
		Pair<Map<Integer, Double>, Dataset<Row>> pair = new Pair<>(muResultsMap,
				df.drop(arguments));
		return pair;
	}
	
	private static double getMultualInformation2(Dataset<Row> df, String firstIndex, String secondIndex,
			double doubleRows, double divEntropy) {

		Function2<Double, Tuple3<Double, Double, Double>, Double> miFunc = new Function2<Double, Tuple3<Double, Double, Double>, Double>() {
			@Override
			public Double call(Double a, Tuple3<Double, Double, Double> x) {
				Double x1 = x._1() / doubleRows;
				a += x1 * Math.log(x1 / (x._2() / doubleRows) / (x._3() / doubleRows));
				return a;
			}
		};

		Dataset<Row> dfCount = df.groupBy(firstIndex, secondIndex).count();

		WindowSpec window0 = Window.partitionBy(firstIndex);
		WindowSpec window1 = Window.partitionBy(secondIndex);
		//dfCount = dfCount.select(functions.col("count"),
		//		functions.regexp_replace(window0.withAggregate(functions.sum("count")), "", "").as("count0"),
		//		functions.regexp_replace(window1.withAggregate(functions.sum("count")), "", "").as("count1"));
		dfCount = dfCount.withColumn("count0",
				 window0.withAggregate(functions.sum("count"))).withColumn("count1",
				 window1.withAggregate(functions.sum("count"))).drop(firstIndex, secondIndex);
		//ReduceFunction<Tuple3<Double, Double, Double>> muFunc = (accum, n) -> (Tuple3.apply(accum._1() + (n._1() * Math.log(n._1() / n._2() / n._3())), 0.0, 0.0));
		Double initialValue = 0.0;
		Double mu = dfCount.as(Encoders.tuple(Encoders.DOUBLE(), Encoders.DOUBLE(), Encoders.DOUBLE())).javaRDD().aggregate(initialValue, miFunc, combine)/*.reduce(muFunc)._1()*/ / Math.log(2);

		if (divEntropy == 0) {
			System.out.println("Division by 0");
			return -1;
		}

		return (2 * mu) / divEntropy;
	}

	private static Dataset<Double> getMultualInformation(Dataset<Row> df, String firstIndex, String secondIndex,
			double doubleRows, double divEntropy) {
		Dataset<Row> dfCount = df.groupBy(firstIndex, secondIndex)
				.agg(functions.count(secondIndex).$div(doubleRows).as("count"));

		WindowSpec window0 = Window.partitionBy(firstIndex);
		WindowSpec window1 = Window.partitionBy(secondIndex);
		dfCount = dfCount.select(/*functions.col(firstIndex), functions.col(secondIndex),*/functions.col("count"),
				functions.regexp_replace(window0.withAggregate(functions.sum("count")), "", "").as("count0"),
				functions.regexp_replace(window1.withAggregate(functions.sum("count")), "", "").as("count1"));
		// TODO: Probar si es mejor calcular la entropía aquí
		// hResults2 = hResults2.union(dfCount.select(functions.col(allColumns[i]),
		// functions.col("count0")).distinct().agg(
		// functions.sum(functions.negate(functions.log("count0").$times(functions.col("count0"))))
		// .$div(Math.log(2)).as("result")));
		
		return dfCount.agg(functions
				.sum(functions.log(functions.col("count").$div(functions.col("count0")).$div(functions.col("count1")))
						.$times(functions.col("count")))
				.$div(Math.log(2)).$times(2).$div(divEntropy)).as("result").as(Encoders.DOUBLE());
	}
	
	private static Pair<Map<Integer, Double>, Dataset<Row>> getAllColumnsEntropy2(Dataset<Row> df, String[] all_columns, double doubleRows) {
		Map<Integer, Double> hResultsMap = new HashMap<Integer, Double>();

		Function2<Double, Double, Double> hFunc = new Function2<Double, Double, Double>() {
			@Override
			public Double call(Double a, Double x) {
				double division = x / doubleRows;
				a -= division * Math.log(division);
				return a;
			}
		};
		List<String> drops = new ArrayList<>();
		int i = 0;
		for (String column : all_columns) {
			Dataset<Row> dfColumn = df.groupBy(column).count();
			//ReduceFunction<Double> hFunc = (accum, n) -> (accum - (Math.log(n)*n));

			Double initialValue = 0.0;
			double entropy = dfColumn.select("count").as(Encoders.DOUBLE()).javaRDD().aggregate(initialValue, hFunc, combine) / Math.log(2);

			hResultsMap.put(i, entropy);

			if(entropy == 0) {
				drops.add(column);
			}
			i++;
		}
		String[] arguments = drops.toArray(new String[drops.size()]);
		System.out.print("Removed 0 entropy features: ");
		System.out.println(Arrays.toString(arguments));
		Pair<Map<Integer, Double>, Dataset<Row>> pair = new Pair<>(hResultsMap, df.drop(arguments));
		return pair;
	}
	
	private static List<Double> getAllColumnsEntropy(Dataset<Row> df, String[] all_columns, double double_rows) {

		Dataset<Double> hResults = spark.createDataset(new ArrayList<>(), Encoders.DOUBLE());
//		int i = 0;
		for (String column : all_columns) {
			String columnResult = column + "_result";
			
			Dataset<Row> dfColumn = df.groupBy(column).agg(functions.count(column).$div(double_rows).as(columnResult));
			
			
			Dataset<Double> aux = dfColumn.agg(
					functions.sum(functions.negate(functions.log(columnResult).$times(functions.col(columnResult))))
							.$div(Math.log(2)).as("result")).as(Encoders.DOUBLE());
			hResults = hResults.union(aux);
			
//			if (i % 5 == 4) {
//				hResults.collect();
//			}
//			i++;
			//ReduceFunction<Double> hFunc = (accum, n) -> (accum - (Math.log(n)*n));
			
			//Double h = dfColumn.select(columnResult).as(Encoders.DOUBLE()).reduce(hFunc) / Math.log(2);
			//System.out.println(h);
			//dfColumn = dfColumn.agg(
			//		functions.sum(functions.negate(functions.log(columnResult).$times(functions.col(columnResult))))
			//				.$div(Math.log(2)).as("result"));
			
			
			//Double value = dfColumn.collectAsList().get(0).getDouble(0);
			//System.out.println(value);
		}
		return hResults.collectAsList();
	}
}
