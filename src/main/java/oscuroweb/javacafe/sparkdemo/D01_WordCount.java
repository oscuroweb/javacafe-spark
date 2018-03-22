package oscuroweb.javacafe.sparkdemo;

import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class D01_WordCount {

    private static final Logger log = Logger.getLogger(D01_WordCount.class.getName());

    public static void main(String[] args) {

    	// Define Spark Configuration
        SparkConf sparkConf = new SparkConf()
        		.setAppName("SparkWordCountDemo");
        
        // Create Spark Context
        JavaSparkContext spark = new JavaSparkContext(sparkConf);
        
        // Read text file to Java RDD
        JavaRDD<String> lines = spark.textFile("spark-warehouse/01_word_count_in.txt");
        
        // Split each lines in words
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 3053478197348611859L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
        
        // Convert to pair where the key is the word and the value is 1
        JavaPairRDD<String, Integer> wordPair = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 951694922796298401L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
        	
		});
        
        // Reduce by key where aggregate values
        JavaPairRDD<String, Integer> wordPairReduced = wordPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 2756582134504452117L;

			@Override
			public Integer call(Integer value1, Integer value2) throws Exception {
				return value1 + value2;
			}
		});
        
        // Switch key to value. The key will be the counter and the value is the word
        JavaPairRDD<Integer, String> wordCountPair = wordPairReduced.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			private static final long serialVersionUID = -890346336952576315L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
				return new Tuple2<Integer, String>(tuple._2, tuple._1);
			}
		});
        
        JavaPairRDD<Integer, String> wordCountPairSorted = wordCountPair.sortByKey(false);
        
        wordCountPairSorted.foreach(tuple -> log.info(tuple._1 + ": " + tuple._2));
        wordCountPairSorted.saveAsTextFile("spark-warehouse/01_word_count_out_" + System.currentTimeMillis());

        spark.stop();

    }
}