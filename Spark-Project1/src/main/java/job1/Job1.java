package job1;

import java.io.Serializable;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Job1 implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private static String pathToFile;
	private static final String PATTERN = ",(?=([^\\\"]*\\\"[^\\\"]*\\\")*(?![^\\\"]*\\\"))";
	private static final String REPLACE = "[\\-\\+\\.\\^:,\"\'$%&(){}£=#@!?\t\n]";
	
	public Job1(String fileInput){
		Job1.pathToFile = fileInput;
	}
	
	public JavaRDD<LinkedList<String>> setup(JavaSparkContext sc) {
		JavaRDD<String> dataWithHeader = sc.textFile(pathToFile);
		String header = dataWithHeader.first();
		JavaRDD<LinkedList<String>> input = dataWithHeader
				.filter(l -> !l.equals(header))
				.map(review -> new LinkedList<>(Arrays.asList(review.split(PATTERN))));
		return input;
	}
	
	public JavaPairRDD<Integer, Map<String, Long>> run(JavaSparkContext sc){
		
		JavaRDD<LinkedList<String>> reviews = setup(sc);
		/**
		 * Parsing lines returning an rdd pairs containing (year, list_of_words)
		 */
		JavaPairRDD<Integer, List<String>> wordsPerYear = reviews
				.mapToPair(line -> {
					String timestamp = line.get(7);
					String summary = line.get(8);
					List<String> tokenized_summary = new ArrayList<>(Arrays.asList(summary.split("\\s+")));
					return new Tuple2<>(getYear(timestamp), tokenized_summary);
				})
				.reduceByKey((l1, l2)-> {l1.addAll(l2); return l1;})
				.filter(tuple -> tuple._1 != -1)
				.sortByKey(true);
		
		/**
		 * Counting, sorting, limit the words list
		 * Map containing (word, count) is used
		 */
		JavaPairRDD<Integer, Map<String, Long>> wordCountPerYear = wordsPerYear.mapValues(words ->{
			Map<String, Long> wordcount = wordcount(words).entrySet().stream()
					.sorted(Entry.comparingByValue(Comparator.reverseOrder()))
					.limit(10)
					.collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
			return wordcount;
		});
		
		return wordCountPerYear;
	}
	
	public Map<String, Long> wordcount(List<String> words){
		Map<String, Long> counts = words.stream()
			    .map(word -> word.replaceAll(REPLACE, "").toLowerCase().trim())
			    .filter(word -> word.length() > 0)
			    .map(word -> new SimpleEntry<>(word, 1))
		        .collect(Collectors.groupingBy( SimpleEntry::getKey, Collectors.counting() ));
		
		return counts;
	}
	
	public int getYear(String timestamp) {
		try {
			long unix_time = Long.parseLong(timestamp);
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
		SparkConf sparkConf = new SparkConf().setAppName("Job1")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.set("spark.kryoserializer.buffer.mb","48")
				;
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		Job1 job = new Job1(args[0]);
		job.run(sc).coalesce(1).saveAsTextFile(args[1]);
		
		sc.close();

	}

}
