package job3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class Job3ProductPairsConfig extends Configured implements Tool {
	public Job3ProductPairsConfig() {
    }
	
	public int run(String[] args) throws Exception {
		 
	    Configuration conf1 = getConf();
	    Job job3_1 = Job.getInstance(conf1);  
	    job3_1.setJobName("products pairs");
        job3_1.setJarByClass(Job3ProductPairsConfig.class);

        job3_1.setMapperClass(Job3ProductPairsMapper.class);
        job3_1.setReducerClass(Job3ProductPairsReducer.class);
        
        job3_1.setMapOutputKeyClass(Text.class);
        job3_1.setMapOutputValueClass(Text.class);
        
        job3_1.setOutputKeyClass(Text.class);
        job3_1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3_1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3_1, new Path(args[1] + "/temp"));
        
        long startTime = System.currentTimeMillis();
        int status = job3_1.waitForCompletion(true) ? 0 : 1;
        System.out.println("Job Finished in "
                + (System.currentTimeMillis() - startTime) / 1000.0
                + " seconds"); 
        
        return status;
		
	}

}
