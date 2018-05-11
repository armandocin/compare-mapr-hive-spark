package job1;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Job1 {
	
	private static String pathToFile;
	private static String pathToOutput;
	private JavaSparkContext sc;
	
	public Job1(String fileInput, String fileOutput){
		Job1.pathToFile = fileInput;
		Job1.pathToOutput = fileOutput;
	}
	
	public JavaSparkContext getContext() {
		return sc;
	}

	public JavaRDD<String> setup(){
		SparkConf sparkConf = new SparkConf().setAppName("Job1");
		sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> input = sc.textFile(pathToFile)
				.flatMap(review -> Arrays.asList(review.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*(?![^\\\"]*\\\"))")).iterator());
		
		return input;
	}
	
	public static void main(String[] args) {
		Job1 job = new Job1(args[0], args[1]);
		
		job.getContext().close();

	}

}
