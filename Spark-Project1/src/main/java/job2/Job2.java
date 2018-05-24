package job2;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Job2 implements Serializable{

	private static final long serialVersionUID = 1L;
	private static String pathToFile;
	private static final Pattern PATTERN = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))");
	
	public Job2(String fileInput){
		Job2.pathToFile = fileInput;
	}
	
	public JavaRDD<LinkedList<String>> setup(JavaSparkContext sc) {
		JavaRDD<String> dataWithHeader = sc.textFile(pathToFile);
		String header = dataWithHeader.first();
		JavaRDD<LinkedList<String>> input = dataWithHeader
				.filter(l -> !l.equals(header))
				.map(review -> new LinkedList<>(Arrays.asList(PATTERN.split(review))));
		return input;
	}
	
	public JavaPairRDD<String, Map<Integer, Double>> run(JavaSparkContext sc){

		JavaRDD<LinkedList<String>> reviews = setup(sc);
		
		Map<Integer, Double> map = new LinkedHashMap<>();
		
		JavaPairRDD<String, Map<Integer, Double>> productMapPair = reviews.mapToPair(
				line -> {
					String product = line.get(1);
					String timestamp = line.get(7);
					Double score = Double.parseDouble(line.get(6));
					
					return new Tuple2<>( new Tuple2<>( product, getYear(timestamp)),  new Tuple2<>(score, 1D) );
				})
				.filter(tuple -> tuple._1._2 >= 2003)
				.reduceByKey((t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2))
				.mapToPair(tuple -> {
					Tuple2<String, Integer> tk = tuple._1;
					Tuple2<Double, Double> tv = tuple._2;
					Double avg = Math.round((tv._1/tv._2)*100.0)/100.0; //rounding
					return new Tuple2<>(tk._1, new Tuple2<>(tk._2, avg));
				})
				.aggregateByKey(map, (m, t) -> putToMap(m,t), (m1,m2) -> mergeMaps(m1,m2))
				.mapValues(m ->{ //sorting map of year-avg by year
					Map<Integer, Double> sorted = m.entrySet().stream()
						.sorted(Entry.comparingByKey())
						.collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
					return sorted;
				})
				.sortByKey(true); //sorting tuple by product
		
		return productMapPair;
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
	
	public Map<Integer, Double> mergeMaps (Map<Integer, Double> m1, Map<Integer, Double> m2) {
		Map<Integer, Double> output = Stream.concat(m1.entrySet().stream(), m2.entrySet().stream())
			    .collect(Collectors.toMap(
			        Entry::getKey,
			        Entry::getValue,
			        (e1,e2)->e2,
			        LinkedHashMap::new
			    )
			);
		return output;
	}
	
	
	public Map<Integer, Double> putToMap (Map<Integer, Double> m, Tuple2<Integer, Double> t){
		m.put(t._1, t._2);
		return m;
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: Job1 <filetxt_input> <filetxt_output>");
			System.exit(1);
		}
		SparkConf sparkConf = new SparkConf().setAppName("Job2")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		Job2 job = new Job2(args[0]);
		job.run(sc).coalesce(1).saveAsTextFile(args[1]);
		
		sc.close();

	}

}
