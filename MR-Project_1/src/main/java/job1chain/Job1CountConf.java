package job1chain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class Job1CountConf extends Configured implements Tool {
	public Job1CountConf() {
    }
	
	public int run(String[] args) throws Exception {
		 
	    Configuration conf1 = getConf();
	    Job job1 = Job.getInstance(conf1);  
	    job1.setJobName("words count");
        job1.setJarByClass(Job1CountConf.class);

        job1.setMapperClass(Job1CountMapper.class);
        job1.setCombinerClass(Job1CountReducer.class);
        job1.setReducerClass(Job1CountReducer.class);
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));
        
        long startTime = System.currentTimeMillis();
        int status = job1.waitForCompletion(true) ? 0 : 1;
        System.out.println("Job Finished in "
                + (System.currentTimeMillis() - startTime) / 1000.0
                + " seconds"); 
        
        return status;
		
	}

}
