package job2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static final Log LOG = LogFactory.getLog(Job2Mapper.class);
	private static List<Integer> FILTERED = new ArrayList<>(Arrays
			.asList(1999, 2000, 2001, 2002));
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		try {
			/*parsing csv records. Form: Id, ProductID, UserID, Profile Name, HelpNum, HelpDen, Score, Time, Summary, Text.*/
			String csv_record = value.toString();
			String[] csv_fields = csv_record.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*(?![^\\\"]*\\\"))");
			String product = csv_fields[1];
			String score = csv_fields[6];
			
			/*parsing Unix time date*/
			Long timestamp = Long.parseLong(csv_fields[7]);
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(timestamp*1000L);
			int year = cal.get(Calendar.YEAR);
			
			String output_value = Integer.toString(year) + "-" + score;
			if(!FILTERED.contains(year))
				context.write(new Text(product), new Text(output_value));
		}
		catch (NumberFormatException e) {
			//System.out.println(value.toString());
			LOG.info("\n" + value.toString() + "\n");
		}
	}

}
