package job3;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job3ProductPairsMapper	extends Mapper<LongWritable, Text, Text, Text> {
	
	private static final Pattern PATTERN = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))");
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		/*parsing csv records. Form: Id, ProductID, UserID, Profile Name, HelpNum, HelpDen, Score, Time, Summary, Text.*/
		String csv_record = value.toString();
		String[] csv_fields = PATTERN.split(csv_record);
		String product = csv_fields[1];
		String user = csv_fields[2];
		
		context.write(new Text(user), new Text(product));
		
	}

}
