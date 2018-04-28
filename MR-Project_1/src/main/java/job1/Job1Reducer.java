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
		Map<String, Long> count = new LinkedHashMap<String, Long>();
		
		for(Text word : values) {
			if (!count.containsKey(word.toString()))
				count.put(word.toString(), one);
			else {
				long occ = count.get(word.toString());
				count.put(word.toString(), occ+1);
			}
		}
		Map<String, Long> sorted = count.entrySet().stream()
				.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
		
		if(sorted.size()>10) {
			Map<String, Long> short_map = new LinkedHashMap<>();
			Iterator<String> it = sorted.keySet().iterator();
			for (int i=0; i<10; i++) {
				String sorted_key = it.next();
				short_map.put(sorted_key, sorted.get(sorted_key));
			}
			context.write(key, new Text(short_map.toString()));
		}
		else
			context.write(key, new Text(sorted.toString()));
		/*
		String output_list = "[";
		for (String keyword : count.keySet()) {
			output_list += keyword + ": " + Long.toString(count.get(keyword)) + ", ";
		}
		output_list = output_list.substring(0, output_list.length()-2) + "]";*/
		
		
	}

}
