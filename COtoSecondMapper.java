import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class COtoSecondMapper extends Mapper<Object, Text, IntWritable, Text> {
	
	// In the mapper, convert the Timesec from float to integer (ceiling)
	// so later in the reducer, things can be grouped by this integer.
	private Text Fieldconcatenated = new Text();
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] arrLine = value.toString().split(",");
		
		// Get the Timesec value from each line
		float Timesec = Float.parseFloat(arrLine[1]);
		
		int Timesecint = (int) Math.ceil(Timesec);  
		// Math.ceil returns a double by default, has to cast it into an int
		
		// treat all the rest of the line as a concatenated string to text
		String stringall = arrLine[2];   // CO
		for (int m = 3; m < 20; m++) {
			stringall = stringall.concat(",");
			stringall = stringall.concat(arrLine[m]);
		}
		// Now stringall is the concatenated string of CO, Ethylene, sr1 to sr16.
		Fieldconcatenated.set(stringall);
		
		context.write(new IntWritable(Timesecint), Fieldconcatenated);

	}

}
