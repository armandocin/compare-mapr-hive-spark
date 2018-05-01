package job3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class Job3UsersCountConfig extends Configured implements Tool {
	public Job3UsersCountConfig() {
    }
	
	@Override
	public int run(String[] args) throws Exception {

        Configuration conf2 = getConf();
        
        Job job3_2 = Job.getInstance(conf2);  
	    job3_2.setJobName("count users");
        job3_2.setJarByClass(Job3UsersCountConfig.class);
        
        job3_2.setMapperClass(Job3UsersCountMapper.class);
        job3_2.setReducerClass(Job3UsersCountReducer.class);
        job3_2.setMapOutputKeyClass(Text.class);
        job3_2.setMapOutputValueClass(Text.class);
        
        job3_2.setOutputKeyClass(Text.class);
        job3_2.setOutputValueClass(LongWritable.class);
        job3_2.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job3_2, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job3_2, new Path(args[1] + "/final"));
        
        long startTime = System.currentTimeMillis();
        int status = job3_2.waitForCompletion(true) ? 0 : 1;
        System.out.println("Job Finished in "
                + (System.currentTimeMillis() - startTime) / 1000.0
                + " seconds"); 
        
        return status;
        
	}

}
