package job1chain;

import org.apache.hadoop.util.ToolRunner;

public class Job1Chain {

	public static void main(String[] args) throws Exception{
		if (args.length < 2) {
			System.err.println("Usage: <path to jar> <filetxt_input> <filetxt_output>");
			System.exit(1);
		}
		
		int exitCode = ToolRunner.run(new Job1CountConf(), args);
		exitCode = ToolRunner.run(new Job1TopNConf(), args);
		System.exit(exitCode);
	}

}
