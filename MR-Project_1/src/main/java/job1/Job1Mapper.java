package job1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Calendar;

public class Job1Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		try {
			/*parsing csv records. Form: Id, ProductID, UserID, Profile Name, HelpNum, HelpDen, Score, Time, Summary, Text.*/
			String csv_record = value.toString();
			String[] csv_fields = csv_record.split(",");
			Long timestamp = Long.parseLong(csv_fields[7]);
			String summary = csv_fields[8];
			summary = summary.toLowerCase();
			
			/*parsing Unix time date*/
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(timestamp*1000L);
			int year = cal.get(Calendar.YEAR);
			
			/*writing the pair (year, word) for each word in the summary*/
			String[] tokenized_summary = summary.split("\\s+");
			for(String word : tokenized_summary) {
				word = word.replaceAll("[\\-\\+\\.\\^:,\"\'$%&(){}£=#@!?\t\n]","");
				context.write(new IntWritable(year), new Text(word));
			}
		}
		catch (NumberFormatException e) {}
		
	}
	
}
