import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class TimeJoinReducer2 extends Reducer<IntWritable, Text, Text, NullWritable> {

	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		int Timesecint = key.get();
		// only write out if key is not zero, that is, only start form timesec=1
		if (Timesecint > 1) {
			String arrLine1 = "";
			String arrLine2 = "";
			String arrLine3 = "";
			for (Text val : values) {
				String[] arrLine = val.toString().split(",");
				int len = arrLine.length;
				// if length is 19, should be on the left,
				// else length is 17, 
				if (len == 19) {
					arrLine1 = val.toString();
				}
				// if the first element is "tn1", should be put second
				// Note that using string.equals() can avoid error caused by using ==
				else if (arrLine[0].equals("tn1")) {
					arrLine2 = val.toString();
					arrLine2 = arrLine2.substring(4);
				}
				// else if the first element is "tn2", should be put third
				else if (arrLine[0].equals("tn2")){
					arrLine3 = val.toString();
					arrLine3 = arrLine3.substring(4);
				}
				//else {continue;}
				
			}
	        // after loop:
			// At most there will be two rounds of loop for each key.
			// the situation when key=0 or 1 is already excluded.
			// need to also exclude when the key is the biggest n+1, n+2
			// where there is no arrLine1 for the key, but no arrLine1

			if (arrLine1.length() > 0) {
				String arrLine123 = arrLine1.concat(",");
				arrLine123 = arrLine123.concat(arrLine2);
				arrLine123 = arrLine123.concat(",");
				arrLine123 = arrLine123.concat(arrLine3);
				context.write(new Text(arrLine123), NullWritable.get());
			}
			// otherwise, do nothing.	
		}
		// otherwise, do nothing.
	}

}
