package job1chain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


public class Job1TopNConf extends Configured implements Tool {
	public Job1TopNConf() {
    }
	
	@Override
	public int run(String[] args) throws Exception {

        Configuration conf2 = getConf();
        
        Job job1 = Job.getInstance(conf2);  
	    job1.setJobName("topN");
        job1.setJarByClass(Job1TopNConf.class);
        
        job1.setMapperClass(Job1TopNMapper.class);
        job1.setReducerClass(Job1TopNReducer.class);
        job1.setCombinerClass(Job1TopNCombiner.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/final"));
        
        long startTime = System.currentTimeMillis();
        int status = job1.waitForCompletion(true) ? 0 : 1;
        System.out.println("Job Finished in "
                + (System.currentTimeMillis() - startTime) / 1000.0
                + " seconds"); 
        
        return status;
        
	}

}
