mvn clean install
spark-submit --class oscuroweb.javacafe.sparkdemo.D05_SparkSQL target/spark-demo-0.0.1-SNAPSHOT.jar spark-warehouse/05_adult_data.csv