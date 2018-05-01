package job2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2Reducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		Map<Integer, List<Double>> scoresCollection = new TreeMap<>();
		
		for(Text info : values) {
			String[] year_score = info.toString().split("-");
			Integer year = Integer.parseInt(year_score[0]);
			Double score = Double.parseDouble(year_score[1]);
			if (!scoresCollection.containsKey(year)) {
				List<Double> scoresList = new ArrayList<>();
				scoresList.add(score);
				scoresCollection.put(year, scoresList);
			}
			else {
				scoresCollection.get(year).add(score);
			}
		}
		
		Map<Integer, Double> avgMap = new TreeMap<>();
		for (Integer y : scoresCollection.keySet()) {
			Long len = scoresCollection.get(y).stream().count();
			Double sum = scoresCollection.get(y).stream().mapToDouble(Double::doubleValue).sum();
			Double avg = (double) (sum/len);
			avg = Math.round(avg*100.0)/100.0; //rounding to 2 decimal points
			avgMap.put(y, avg);
		}
		
		context.write(key, new Text(avgMap.entrySet().toString()));
	}

}
