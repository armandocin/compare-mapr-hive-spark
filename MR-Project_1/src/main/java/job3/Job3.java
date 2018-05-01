package job3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job3 {

	public static void main(String[] args) throws Exception{
		Path out = new Path(args[1]);
		
		Configuration conf = new Configuration();
        Job job3_1 = Job.getInstance(conf, "job3_1");
        job3_1.setJarByClass(Job3.class);
        job3_1.setMapperClass(Job3Mapper.class);
        job3_1.setReducerClass(Job3Reducer.class);
        
        job3_1.setMapOutputKeyClass(Text.class);
        job3_1.setMapOutputValueClass(Text.class);
        
        job3_1.setOutputKeyClass(Text.class);
        job3_1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3_1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3_1, new Path(out, "temp"));
        
        long startTime = System.currentTimeMillis();
        
        if (!job3_1.waitForCompletion(true)) {
        	  System.exit(1);
        }
//        System.exit(job3_1.waitForCompletion(true) ? 0 : 1);
        
        Job job3_2 = Job.getInstance(conf, "job3_2");
        job3_2.setJarByClass(Job3.class);
        job3_2.setMapperClass(Job3IdentityMapper.class);
        job3_1.setReducerClass(Job3UsersCountReducer.class);
        
        job3_2.setOutputKeyClass(Text.class);
        job3_2.setOutputValueClass(LongWritable.class);
        job3_2.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job3_2, new Path(out, "temp"));
        FileOutputFormat.setOutputPath(job3_2, new Path(out, "final"));
        
        
        
        int status = job3_2.waitForCompletion(true) ? 0 : 1;

        System.out.println("Job Finished in "
                + (System.currentTimeMillis() - startTime) / 1000.0
                + " seconds");
        
        System.exit(status);

	}

}
