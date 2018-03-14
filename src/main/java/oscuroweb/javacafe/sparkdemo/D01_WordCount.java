package oscuroweb.javacafe.sparkdemo;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
        
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        
        JavaPairRDD<String, Integer> wordPair = words
        		.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
        
        Long totalCount = words.count();
        
        System.out.println("Total count: " + totalCount);
        
        JavaPairRDD<String, Integer> wordPairReduced = wordPair
        		.reduceByKey( (value1, value2) -> value1 + value2);
        
        List<Tuple2<Integer, String>> wordCountPair = wordPairReduced
        		.mapToPair(tuple -> new Tuple2<Integer, String>(tuple._2, tuple._1))
        		.sortByKey(false)
        		.collect();
        
        
        wordCountPair.stream().forEach(tuple -> System.out.println(tuple._1 + ": " + tuple._2));

        spark.stop();

    }
}