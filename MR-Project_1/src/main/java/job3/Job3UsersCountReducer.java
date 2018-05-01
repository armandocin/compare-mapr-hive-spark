package job3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3UsersCountReducer extends Reducer<Text, Text, Text, LongWritable> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		int count = 0;
		Iterator<Text> it = values.iterator();
		while(it.hasNext()) {
			count += 1;
		}
		
		context.write(key, new LongWritable(count));
		
	}

}