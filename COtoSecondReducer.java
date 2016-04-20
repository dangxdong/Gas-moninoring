import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class COtoSecondReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		int Timesecint = key.get();
		
		int count = 0;
		float[] fieldsums = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}; 
		for (Text val : values) {
            // each val is the concatenation of a line from the original file,
			// including the readings of of CO, Ethylene, sr1 to sr16.
			// has to get all the 18 fields out, calculate the average for each field.
			
			String[] arrLine1 = val.toString().split(",");
			
			for (int ii=0; ii<18; ii++) {
				float flt = Float.parseFloat(arrLine1[ii]);
				fieldsums[ii] = fieldsums[ii] + flt;
			}
			
			count++;
		}
		
		float[] fieldavg = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
		
		// if Timesec is zero, use the sum as the average.
		if (Timesecint == 0) {
			for (int nn=0; nn<18; nn++) {
				fieldavg[nn] = fieldsums[nn];
			}
		}
		// if Timesec is nonzero, calculate the real average.
		else {
			for (int nn=0; nn<18; nn++) {
				fieldavg[nn] = fieldsums[nn] / count;
			}
		}
		
		
		// Now the average has been obtained, but need to write them out as text:
		
		String stringout = Float.toString(fieldavg[0]);
		
		for (int jj = 1; jj < 18; jj++) {
			stringout = stringout.concat(",");
			stringout = stringout.concat(Float.toString(fieldavg[jj]));
		}
		
		// The integer Timesec should also be written within the text

		String Timesecstr = key.toString();
		Timesecstr = Timesecstr.concat(",");
		stringout = Timesecstr.concat(stringout);
		
		context.write(new Text(stringout), NullWritable.get());
	}

}
