package job3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
//import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
//import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Job3 extends Configured implements Tool {
	private Job3() {
    }
	
	public int run(String[] args) throws Exception {
		
//		JobControl jobControl = new JobControl("jobChain"); 
	    Configuration conf1 = getConf();
	    Job job3_1 = Job.getInstance(conf1);  
	    job3_1.setJobName("products pairs");
        job3_1.setJarByClass(Job3.class);

        job3_1.setMapperClass(Job3Mapper.class);
        job3_1.setReducerClass(Job3Reducer.class);
        
        job3_1.setMapOutputKeyClass(Text.class);
        job3_1.setMapOutputValueClass(Text.class);
        
        job3_1.setOutputKeyClass(Text.class);
        job3_1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3_1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3_1, new Path(args[1] + "/temp"));
        
//        ControlledJob controlledJob1 = new ControlledJob(conf1);
//        controlledJob1.setJob(job3_1);
//
//        jobControl.addJob(controlledJob1);
//        Configuration conf2 = getConf();
//        
//        Job job3_2 = Job.getInstance(conf2);  
//	    job3_2.setJobName("count users");
//        job3_2.setJarByClass(Job3.class);
//        
//        job3_2.setMapperClass(Job3IdentityMapper.class);
//        job3_2.setReducerClass(Job3UsersCountReducer.class);
//        
//        job3_2.setOutputKeyClass(Text.class);
//        job3_2.setOutputValueClass(LongWritable.class);
//        job3_2.setInputFormatClass(KeyValueTextInputFormat.class);
//        FileInputFormat.addInputPath(job3_2, new Path(args[1] + "/temp"));
//        FileOutputFormat.setOutputPath(job3_2, new Path(args[1] + "/final"));
//        
//        ControlledJob controlledJob2 = new ControlledJob(conf2);
//        controlledJob2.setJob(job3_2);
//        
//        // make job2 dependent on job1
//        controlledJob2.addDependingJob(controlledJob1); 
//        // add the job to the job control
//        jobControl.addJob(controlledJob2);
//        Thread jobControlThread = new Thread(jobControl);
//        jobControlThread.start();
//        //System.exit(0); 
        
        long startTime = System.currentTimeMillis();
        int status = job3_1.waitForCompletion(true) ? 0 : 1;
        System.out.println("Job Finished in "
                + (System.currentTimeMillis() - startTime) / 1000.0
                + " seconds"); 
        
        return status;
		
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Job3(), args);
		System.exit(exitCode);
	}

}
