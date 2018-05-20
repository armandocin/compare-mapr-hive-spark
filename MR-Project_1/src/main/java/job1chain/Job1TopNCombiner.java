package job1chain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1TopNCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		List<String> list = new ArrayList<>();
		values.forEach(e -> list.add(e.toString()));
		list.stream()
				.sorted(new TopNComparator())
				.limit(10)
				.forEachOrdered(e -> {
					try {
						context.write(key, new Text(e));
					} catch (IOException | InterruptedException e1) {
						e1.printStackTrace();
					}
				});
			
	}

}
