package job2;

import java.io.Serializable;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Job2 implements Serializable{

	private static final long serialVersionUID = 1L;
	private static String pathToFile;
	private static final String PATTERN = ",(?=([^\\\"]*\\\"[^\\\"]*\\\")*(?![^\\\"]*\\\"))";
	private static final String REPLACE = "[\\-\\+\\.\\^:,\"\'$%&(){}£=#@!?\t\n]";
	
	public Job2(String fileInput){
		Job2.pathToFile = fileInput;
	}
	
	public JavaRDD<LinkedList<String>> setup(JavaSparkContext sc) {
		JavaRDD<LinkedList<String>> input = sc.textFile(pathToFile)
				.map(review -> new LinkedList<>(Arrays.asList(review.split(PATTERN))));
		return input;
	}
	
	public JavaPairRDD<String, Map<Integer, Double>> run(JavaSparkContext sc){
		
		JavaRDD<LinkedList<String>> reviews = setup(sc);
		
		JavaPairRDD<Object, Object> first = reviews.mapToPair(
				line -> {
					String product = line.get(1);
					String timestamp = line.get(7);
					Double score = Double.parseDouble(line.get(6));
					
					return new Tuple2<>( product, new Tuple2<>( getYear(timestamp), score ) );
				});
		
		return null;
	}
	
	public Integer getYear(String timestamp) {
		try {
			Long unix_time = Long.parseLong(timestamp);
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(unix_time*1000L);
			int year = cal.get(Calendar.YEAR);
			
			return year;
		}catch (NumberFormatException e) {
			return -1;
		}
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: Job1 <filetxt_input> <filetxt_output>");
			System.exit(1);
		}
		SparkConf sparkConf = new SparkConf().setAppName("Job1");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		sc.close();

	}

}
