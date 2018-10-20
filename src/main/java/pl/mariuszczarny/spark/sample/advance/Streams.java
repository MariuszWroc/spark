package pl.mariuszczarny.spark.sample.advance;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Streams {

	public static void main(String[] args) {
				SparkConf conf = new SparkConf().setAppName("Mariusz");
				JavaSparkContext sc = new JavaSparkContext(conf);
				List<Integer> squares = sc.parallelize(Arrays.asList(1, 2, 3))
				        .map(n -> n*n)
				        .collect();
				System.out.println(squares.toString());
				sc.close();
				// Rough equivalent using Java Streams
				List<Integer> squares2 = Stream.of(1, 2, 3)
				        .map(n -> n*n)
				        .collect(Collectors.toList());
				System.out.println(squares2.toString());
	}

}
