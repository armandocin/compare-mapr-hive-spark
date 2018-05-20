package job1chain;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1TopNReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		List<String> list = new ArrayList<>();
		values.forEach(e -> list.add(e.toString()));
		List<String> topN = list.stream()
				.sorted(new TopNComparator())
				.limit(10)
				.collect(Collectors.toList());
		
		context.write(key, new Text(topN.toString()));
	}

}
