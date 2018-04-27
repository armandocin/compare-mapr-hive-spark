package job1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Calendar;

public class Job1Mapper extends Mapper<Object, Text, IntWritable, Text> {
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		/*parsing csv records. Form: Id, ProductID, UserID, Profile Name, HelpNum, HelpDen, Score, Time, Summary, Text.*/
		String csv_record = value.toString();
		String[] csv_fields = csv_record.split(",");
		Long timestamp = Long.parseLong(csv_fields[7]);
		String summary = csv_fields[8];
		
		/*parsing unix time date*/
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(timestamp*1000L);
		int year = cal.get(Calendar.YEAR);
		
		/*counting words occurences in summary field*/
		String[] tokenized_summary = summary.split("\\s");
		for(String token : tokenized_summary) {
			token = token.replaceAll("[\\-\\+\\.\\^:,\"\'$%&(){}£=#@]","");
		}
		
	}
	
}
