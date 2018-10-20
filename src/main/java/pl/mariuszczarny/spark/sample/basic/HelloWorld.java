package pl.mariuszczarny.spark.sample.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
 
public class HelloWorld {
 
	public static void main(String[] args) {
 
		SparkConf sparkConf = new SparkConf();
 
		sparkConf.setAppName("Hello Spark");
		sparkConf.setMaster("local");
 
		JavaSparkContext context = new JavaSparkContext(sparkConf);
 
		//...
 
		context.close();
 
	}
}
