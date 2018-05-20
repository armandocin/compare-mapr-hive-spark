package job3;

import org.apache.hadoop.util.ToolRunner;

public class Job3 {

	public static void main(String[] args) throws Exception{
		if (args.length < 2) {
			System.err.println("Usage: <path to jar> <filetxt_input> <filetxt_output>");
			System.exit(1);
		}
		int exitCode = ToolRunner.run(new Job3ProductPairsConfig(), args);
		exitCode = ToolRunner.run(new Job3UsersCountConfig(), args);
		System.exit(exitCode);
	}

}
