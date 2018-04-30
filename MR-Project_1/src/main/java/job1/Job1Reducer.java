package job1;

import java.io.IOException;
import java.util.*;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	
	private static long one = 1;
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Map<String, Long> count = new LinkedHashMap<>();
		
		for(Text word : values) {
			if (!count.containsKey(word.toString()))
				count.put(word.toString(), one);
			else {
				long occ = count.get(word.toString());
				count.put(word.toString(), occ+1);
			}
		}
		Map<String, Long> sorted = count.entrySet().stream()
				.sorted(Entry.comparingByValue(Comparator.reverseOrder()))
				.limit(10)
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
		
		context.write(key, new Text(sorted.entrySet().toString()));
		/*
		String output_list = "[";
		for (String keyword : count.keySet()) {
			output_list += keyword + ": " + Long.toString(count.get(keyword)) + ", ";
		}
		output_list = output_list.substring(0, output_list.length()-2) + "]";*/
		
		
	}

}
