package job3;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import job1.Job1Mapper;

public class Job3Mapper	extends Mapper<LongWritable, Text, Text, Text> {
	
	private static final Log LOG = LogFactory.getLog(Job1Mapper.class);
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		try {
			/*parsing csv records. Form: Id, ProductID, UserID, Profile Name, HelpNum, HelpDen, Score, Time, Summary, Text.*/
			String csv_record = value.toString();
			String[] csv_fields = csv_record.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*(?![^\\\"]*\\\"))");
			String product = csv_fields[1];
			String user = csv_fields[2];

			context.write(new Text(user), new Text(product));
		}
		catch (NumberFormatException e) {
			//System.out.println(value.toString());
			LOG.info("\n" + value.toString() + "\n");
		}
	}

}
