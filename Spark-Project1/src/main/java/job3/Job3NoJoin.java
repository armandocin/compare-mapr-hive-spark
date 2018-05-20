package job3;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

//import job3.TupleComparator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Job3NoJoin implements Serializable {

	private static final long serialVersionUID = 1L;
	private static String pathToFile;
	private static final Pattern PATTERN = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))");
	
	public Job3NoJoin(String fileInput){
		Job3NoJoin.pathToFile = fileInput;
	}
	
	public JavaRDD<LinkedList<String>> setup(JavaSparkContext sc) {
		JavaRDD<String> dataWithHeader = sc.textFile(pathToFile,4);
		String header = dataWithHeader.first();
		JavaRDD<LinkedList<String>> input = dataWithHeader
				.filter(l -> !l.equals(header))
				.map(review -> new LinkedList<>(Arrays.asList(PATTERN.split(review))));
		return input;
	}
	
	public JavaPairRDD<String, Long> run(JavaSparkContext sc) {
		
		JavaRDD<LinkedList<String>> reviews = setup(sc);
		
		JavaPairRDD<String, Long> commonUsersRDD = reviews.mapToPair(
				line -> {
					String product = line.get(1);
					String user = line.get(2);
					
					return new Tuple2<>( user, product );
				})
				.groupByKey()
				.mapValues(products_list -> {
					Set<String> set = new HashSet<>(); //no duplicates
					products_list.forEach(prod -> set.add(prod.toString()));
					List<String> list = new ArrayList<>();
					list.addAll(set);
					List<String> sorted = list
							.stream()
							.sorted()
							.collect(Collectors.toCollection(LinkedList::new));
					return sorted;
					
				})
				.filter(tuple -> tuple._2.size()>1)
				.flatMapToPair(tuple -> {
						List<Tuple2<String, String>> productPairs = new ArrayList<>();
						for(String prev : tuple._2) {
							Iterator<String> it1 = tuple._2.listIterator(tuple._2.indexOf(prev)+1);
							it1.forEachRemaining(curr -> {
								String pair = prev +","+ curr;
								productPairs.add(new Tuple2<>(pair, tuple._1));
							});
						}
						return productPairs.iterator();
				})
				.aggregateByKey(0L, (acc, user) -> acc+1L, (p1, p2) -> p1+p2)
				.sortByKey();
				
		return commonUsersRDD;

	}
	
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: Job1 <filetxt_input> <filetxt_output>");
			System.exit(1);
		}
		SparkConf sparkConf = new SparkConf().setAppName("Job3NoJoin");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		Job3NoJoin job = new Job3NoJoin(args[0]);
		job.run(sc).coalesce(1).saveAsTextFile(args[1]);
		
		sc.close();

	}

}
