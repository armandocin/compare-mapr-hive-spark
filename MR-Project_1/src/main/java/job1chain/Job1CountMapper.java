package job1chain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job1CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	private static final Log LOG = LogFactory.getLog(Job1CountMapper.class);
	private static List<String> FILTERED = new ArrayList<>(Arrays
			.asList("")//, "is", "are", "this", "these", "that", "but", "the", "and", "a", "to", "in", "an", "for", "by", "of", "from", "with", "on", "i", "not", "it", "my")
			);
	private static final Pattern PATTERN = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))");
	private static final LongWritable one = new LongWritable(1);
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		try {
			/*parsing csv records. Form: Id, ProductID, UserID, Profile Name, HelpNum, HelpDen, Score, Time, Summary, Text.*/
			String csv_record = value.toString();
			String[] csv_fields = PATTERN.split(csv_record);
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
				if( !FILTERED.contains(word) ) {
					String concat = Integer.toString(year) + "-" + word;
					context.write(new Text(concat), one);
				}
			}
		}
		catch (NumberFormatException e) {
			//System.out.println(value.toString());
			LOG.info("\n" + value.toString() + "\n");
		}
		
	}
	
	
	
}
