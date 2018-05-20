package job1chain;

import java.util.Comparator;

public class TopNComparator implements Comparator<String> {
	
	@Override
	public int compare(String arg0, String arg1) {
		String[] div0 = arg0.split("=");
		int count0 = Integer.parseInt(div0[0]);
		String word0 = div0[1];
		String[] div1 = arg1.split("=");
		int count1 = Integer.parseInt(div1[0]);
		String word1 = div1[1];
		if(count1 == count0) {
			return word0.compareTo(word1);
		}
		return count1 - count0;
	}

}
