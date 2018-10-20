package pl.mariuszczarny.spark.sample.basic;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TakeSample {
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName("Hello Spark");
		sparkConf.setMaster("local");

		JavaSparkContext context = new JavaSparkContext(sparkConf);

		JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(8, 0, 5, 3, 10, 6));

		List<Integer> numbersList = numbersRDD.take(3);
		System.out.println(numbersList.toString());

		context.close();

	}
}
