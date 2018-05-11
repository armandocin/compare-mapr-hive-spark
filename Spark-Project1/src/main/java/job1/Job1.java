package job1;

import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Job1 {
	
	private static String pathToFile;
	private static String pathToOutput;
	private JavaSparkContext sc;
	private static final String PATTERN = ",(?=([^\\\"]*\\\"[^\\\"]*\\\")*(?![^\\\"]*\\\"))";
	private static final String REPLACE = "[\\-\\+\\.\\^:,\"\'$%&(){}£=#@!?\t\n]";
	
	public Job1(String fileInput, String fileOutput){
		Job1.pathToFile = fileInput;
		Job1.pathToOutput = fileOutput;
	}
	
	public JavaSparkContext getContext() {
		return sc;
	}

	public JavaRDD<List<String>> setup(){
		SparkConf sparkConf = new SparkConf().setAppName("Job1");
		sc = new JavaSparkContext(sparkConf);
		JavaRDD<List<String>> input = sc.textFile(pathToFile)
				.map(review -> Arrays.asList(review.split(PATTERN)));
		
		return input;
	}
	
	public JavaPairRDD<Integer, Map<String, Long>> mapper(){
		/**
		 * Parsing lines returning an rdd pairs containing (year, list_of_words)
		 */
		JavaRDD<List<String>> reviews = setup();
		JavaPairRDD<Integer, List<String>> wordsPerYear = reviews
				.mapToPair(line -> {
					String timestamp = line.get(7);
					String summary = line.get(8);
					List<String> tokenized_summary = Arrays.asList(summary.split("\\s+"));
					return new Tuple2<>(getYear(timestamp), tokenized_summary);
				})
				.reduceByKey((l1, l2)-> {l1.addAll(l2); return l1;});
		
		/**
		 * Counting, sorting, limit the words list
		 * Map containing (word, count) is used
		 */
		JavaPairRDD<Integer, Map<String, Long>> wordCountPerYear = wordsPerYear.mapToPair(tuple ->{
			Map<String, Long> wordcount = wordcount(tuple._2).entrySet().stream()
					.sorted(Entry.comparingByValue(Comparator.reverseOrder()))
					.limit(10)
					.collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));;
			return new Tuple2<>(tuple._1, wordcount);
		});
		
		return wordCountPerYear;
	}
	
	
	
	public Map<String, Long> wordcount(List<String> words){
		Map<String, Long> counts = words.stream()
			    .map(word -> word.replaceAll(REPLACE, "").toLowerCase().trim())
			    .map(word -> new SimpleEntry<>(word, 1))
		        .collect(Collectors.groupingBy( SimpleEntry::getKey, Collectors.counting() ));
		
		return counts;
	}
	
	public Integer getYear(String timestamp) {
		Long unix_time = Long.parseLong(timestamp);
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(unix_time*1000L);
		int year = cal.get(Calendar.YEAR);
		
		return year;
	}
	
	public static void main(String[] args) {
		Job1 job = new Job1(args[0], args[1]);
		
		job.getContext().close();

	}

}
