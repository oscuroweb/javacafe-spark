package oscuroweb.javacafe.sparkdemo;

import java.util.Arrays;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class D02_WordCountLambda {

    private static final Logger log = Logger.getLogger(D02_WordCountLambda.class.getName());

    public static void main(String[] args) {

    		// Define Spark Configuration
        SparkConf sparkConf = new SparkConf()
        		.setMaster("local[2]")
        		.setAppName("SparkWordCountLambdaDemo");
        
        // Create Spark Context
        JavaSparkContext spark = new JavaSparkContext(sparkConf);
        
        // Read text file to Java RDD
        JavaRDD<String> lines = spark.textFile("spark-warehouse/01_word_count_in.txt");
        
        // Word count with lambda
        JavaPairRDD<Integer, String> wordCountPair = lines
        		.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
	        .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
        		.reduceByKey( (value1, value2) -> value1 + value2)
        		.mapToPair(tuple -> new Tuple2<Integer, String>(tuple._2, tuple._1))
        		.sortByKey(false);
        
        wordCountPair.foreach(tuple -> log.info(tuple._1 + ": " + tuple._2));
        wordCountPair.saveAsTextFile("spark-warehouse/02_word_count_out");
        
        spark.stop();

    }
}