package job3;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

@SuppressWarnings("serial")
class TupleComparator implements Comparator<Tuple2<String, String>>, Serializable {

	@Override
	public int compare(Tuple2<String, String> t1, Tuple2<String, String> t2) {
	    if (t1._1.compareTo(t2._1)==0)
	        return t1._2.compareTo(t2._2);
	    return t1._1.compareTo(t2._1);
	}
}
