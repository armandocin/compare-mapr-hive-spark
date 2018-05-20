package job1chain;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job1TopNMapper extends Mapper<Text, Text, IntWritable, Text> {
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		String keyString = key.toString();
		String[] keySeparate = keyString.split("-");
		IntWritable year = new IntWritable(Integer.parseInt(keySeparate[0]));
		String word = keySeparate[1];
		
		String wordCnt = value.toString()+ "=" + word;
		
		context.write(year, new Text(wordCnt));
		
	}

}
