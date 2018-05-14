package job3;

import java.io.Serializable;
import java.util.*;
import job3.TupleComparator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Job3 implements Serializable {

	private static final long serialVersionUID = 1L;
	private static String pathToFile;
	private static final String PATTERN = ",(?=([^\\\"]*\\\"[^\\\"]*\\\")*(?![^\\\"]*\\\"))";
	
	public Job3(String fileInput){
		Job3.pathToFile = fileInput;
	}
	
	public JavaRDD<LinkedList<String>> setup(JavaSparkContext sc) {
		JavaRDD<String> dataWithHeader = sc.textFile(pathToFile);
		String header = dataWithHeader.first();
		JavaRDD<LinkedList<String>> input = dataWithHeader
				.filter(l -> !l.equals(header))
				.map(review -> new LinkedList<>(Arrays.asList(review.split(PATTERN))));
		return input;
	}
	
	public JavaPairRDD<Tuple2<String, String>, Long> run(JavaSparkContext sc) {
		
		JavaRDD<LinkedList<String>> reviews = setup(sc);
		
		JavaPairRDD<String, String> prodUserPairRDD = reviews.mapToPair(
				line -> {
					String product = line.get(1);
					String user = line.get(2);
					
					return new Tuple2<>( user, product );
				}).distinct();
		
		JavaPairRDD<Tuple2<String, String>, Long> commonUsersRDD = prodUserPairRDD
				.join(prodUserPairRDD)
				.filter(tuple -> tuple._2._1.compareTo(tuple._2._2)<0) //take only pairs where products are different and the first precedes the latter
				.mapToPair(tuple -> new Tuple2<>( tuple._2, 1L ))
				//.aggregateByKey(0L, (a, b) -> a+1L, (p1, p2) -> p1+p2);
				.reduceByKey((a,b) -> a+b)
				.sortByKey(new TupleComparator())
				;
				
		return commonUsersRDD;

	}
	
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: Job1 <filetxt_input> <filetxt_output>");
			System.exit(1);
		}
		SparkConf sparkConf = new SparkConf().setAppName("Job3");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		Job3 job = new Job3(args[0]);
		job.run(sc).coalesce(1).saveAsTextFile(args[1]);
		
		sc.close();

	}

}
