package job1;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Job1 implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private static final long one = 1L;
	private static String pathToFile;
	private static final Pattern PATTERN = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))");
	private static final String REPLACE = "[\\-\\+\\.\\^:|\\*,\"\'$%&(){}£=#@!?\t\n]";
	
	public Job1(String fileInput){
		Job1.pathToFile = fileInput;
	}
	
	public JavaRDD<LinkedList<String>> setup(JavaSparkContext sc) {
		JavaRDD<String> dataWithHeader = sc.textFile(pathToFile, 4);
		String header = dataWithHeader.first();
		JavaRDD<LinkedList<String>> input = dataWithHeader
				.filter(l -> !l.equals(header))
				.map(review -> new LinkedList<>(Arrays.asList(PATTERN.split(review))));
		return input;
	}
	
public JavaPairRDD<Integer, List<String>> run(JavaSparkContext sc){
		
		JavaRDD<LinkedList<String>> reviews = setup(sc);
		
		JavaPairRDD<Integer, List<String>> wordCountPerYear = reviews
				.flatMapToPair(line -> {
					String timestamp = line.get(7);
					String summary = line.get(8);
					List<String> tokenized_summary = new ArrayList<>(Arrays.asList(summary.split("\\s+")));
					String year = Integer.toString(getYear(timestamp));
					
					List<Tuple2<String, Long>> tupleList = new ArrayList<>();
					for(String word : tokenized_summary) {
						word = word.replaceAll(REPLACE, "").toLowerCase().trim();
						if(word.length()>0 && year.compareTo("-1")!=0)
							tupleList.add(new Tuple2<>(year+"-"+word, one));
					}
					return tupleList.iterator();
				})
				.reduceByKey((a, b)-> a + b)
				.mapToPair(tuple ->{
					String[] keySeparate = tuple._1.split("-");
					int year = Integer.parseInt(keySeparate[0]);
					String word = keySeparate[1];
					
					return new Tuple2<>(year, word+"="+Long.toString(tuple._2));			
					
				})
				.groupByKey()
				.mapValues(iterable -> {
					List<String> list = new ArrayList<>();
					Iterator<String> it = iterable.iterator();
					while(it.hasNext()) {
						list.add(it.next().toString());
					}
					List<String> topN = list.stream()
							.sorted(new TopNComparator())
							.limit(10)
							.collect(Collectors.toList());
					return topN;
				})
				.sortByKey(true);
		
		return wordCountPerYear;
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
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		Job1 job = new Job1(args[0]);
		job.run(sc).coalesce(1).saveAsTextFile(args[1]);
		
		sc.close();

	}

}
