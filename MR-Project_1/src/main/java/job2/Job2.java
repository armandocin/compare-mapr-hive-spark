package job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job2 {

	public static void main(String[] args) throws Exception{
		if (args.length < 2) {
			System.err.println("Usage: <path to jar> <filetxt_input> <filetxt_output>");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job2");
        job.setJarByClass(Job2.class);
        job.setMapperClass(Job2Mapper.class);
        job.setReducerClass(Job2Reducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        long startTime = System.currentTimeMillis();
        
        int status = job.waitForCompletion(true) ? 0 : 1;

        System.out.println("Job Finished in "
                + (System.currentTimeMillis() - startTime) / 1000.0
                + " seconds");
        
        System.exit(status);

	}

}
