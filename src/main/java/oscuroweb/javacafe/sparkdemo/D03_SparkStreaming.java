package oscuroweb.javacafe.sparkdemo;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class D03_SparkStreaming {
	
	public static void main(String[] args) throws InterruptedException {
		
		
		// Create a local StreamingContext with two 
		// working thread and batch interval of 1 second
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("SparkStreamingWordCount");
		
		
		JavaStreamingContext sparkStreamingContext = 
				new JavaStreamingContext(conf, Durations.seconds(10));
		
	
		// Create a DStream that will connect to hostname:port
		JavaDStream<String> lines = sparkStreamingContext
				.socketTextStream("localhost", 9999);
		
		// Count each word in each batch
		JavaPairDStream<String, Integer> wordCounts = lines
				.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
				.mapToPair(word -> new Tuple2<String, Integer>(word, 1))
				.reduceByKey((value1, value2) -> value1 + value2);
		
		wordCounts.print();
		
		// Start the computation
		sparkStreamingContext.start();
		
		// Wait for the computation to terminate
		sparkStreamingContext.awaitTermination();
		
	}

}
