mvn clean install
spark-submit --class oscuroweb.javacafe.sparkdemo.D01_WordCount --master local[2] target/spark-demo-0.0.1-SNAPSHOT.jar