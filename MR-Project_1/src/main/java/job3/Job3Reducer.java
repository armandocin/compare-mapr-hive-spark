package job3;

import java.util.*;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3Reducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		List<String> values_list = new ArrayList<>();
		Set<String> values_set = new HashSet<>(); //no duplicates
		values.forEach(e -> values_set.add(e.toString()));
		values_list.addAll(values_set);
		
		for(String prev : values_list) {
			Iterator<String> it1 = values_list.listIterator(values_list.indexOf(prev)+1);
			it1.forEachRemaining(curr -> {
				String pair = "("+ prev +","+ curr +")";
				try {
					context.write(new Text(pair), key);
				} catch (IOException | InterruptedException e1) {
					e1.printStackTrace();
				}
			});
		}
		
	}

}
