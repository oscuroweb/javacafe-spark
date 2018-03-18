package oscuroweb.javacafe.sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;

public class D05_SparkSQL {
	
	public static final void main(String[] args) {
		
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkSQL Demo")
                .setMaster("local[2]");
		
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .sparkContext(sparkContext.sc())
				  .getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");

		// Get dataset from csv file
		Dataset<Row> dataset = spark.read()
				.option("ignoreLeadingWhiteSpace", true)
				.csv(args[0]);
		
		dataset.show();
		
		// Rename dataset columns
		dataset = dataset.withColumnRenamed("_C0", "age")
				.withColumnRenamed("_C1", "workclass")
				.withColumnRenamed("_C2", "fnlwgt")
				.withColumnRenamed("_C3", "education")
				.withColumnRenamed("_C4", "education-num")
				.withColumnRenamed("_C5", "marital-status")
				.withColumnRenamed("_C6", "occupation")
				.withColumnRenamed("_C7", "relationship")
				.withColumnRenamed("_C8", "race")
				.withColumnRenamed("_C9", "sex")
				.withColumnRenamed("_C10", "capital-gain")
				.withColumnRenamed("_C11", "capital-loss")
				.withColumnRenamed("_C12", "hours-per-week")
				.withColumnRenamed("_C13", "native-country")
				.withColumnRenamed("_C14", "income");
		
		dataset.show();
		
		// Binarize income column. 
		// 1 => >50K
		// 0 => <=50K
		Dataset<Row> binarizeIncomeDataset = dataset
				.withColumn("income_bin", dataset.col("income").equalTo(">50K")
						.cast(DataTypes.IntegerType));
		
		binarizeIncomeDataset.show();

		// Average age for each income value
		binarizeIncomeDataset = binarizeIncomeDataset
				.withColumn("age", binarizeIncomeDataset.col("age")
						.cast(DataTypes.DoubleType));
		
		Dataset<Row> avgAgeDataset = binarizeIncomeDataset
				.groupBy("income_bin")
				.avg("age");
		
		avgAgeDataset.show();
		
		// Average age for each income value SQL
		binarizeIncomeDataset.createOrReplaceTempView("dataset");
		
		Dataset<Row> avgAgeSQLDataset = spark.sql(
				"SELECT income_bin, AVG(age) "
				+ "FROM dataset "
				+ "GROUP BY income_bin");
		
		avgAgeSQLDataset.show();
		
		sparkContext.close();
		spark.close();

		
	}

}
