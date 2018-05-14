package job3;

import java.util.*;
import java.util.stream.Collectors;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3ProductPairsReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		List<String> values_list = new LinkedList<>();
		Set<String> values_set = new HashSet<>(); //no duplicates
		values.forEach(e -> values_set.add(e.toString()));
		values_list.addAll(values_set);
		List<String> sorted_values = values_list.stream().sorted().collect(Collectors.toCollection(LinkedList::new));
		
		for(String prev : sorted_values) {
			Iterator<String> it1 = sorted_values.listIterator(sorted_values.indexOf(prev)+1);
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
