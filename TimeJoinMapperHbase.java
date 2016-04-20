import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TimeJoinMapperHbase extends Mapper<Object, Text, IntWritable, Text> {
	
	private Text Fieldconcatenated = new Text();
	private Text Fieldconcatenated2 = new Text();
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
        
		String[] arrLine = value.toString().split(",");
		
		int Timesec = Integer.parseInt(arrLine[0]);
		
		String stringall = arrLine[0];   // the concatenated string should include the Timesec
		for (int m = 1; m < 19; m++) {
			stringall = stringall.concat(",");
			stringall = stringall.concat(arrLine[m]);
		}
		// Now stringall is the concatenated string of CO, Ethylene, sr1 to sr16.
		Fieldconcatenated.set(stringall);
		
		// write the key_value for time t
		context.write(new IntWritable(Timesec), Fieldconcatenated);
		
		// But also write another concatenated string, with a different key
		// to be used by t+1
		
		//concatenated string without the Timesec, CO and Ethylene, start from the fourth field
		String stringall2 = arrLine[3];   
		for (int m = 4; m < 19; m++) {
			stringall2 = stringall2.concat(",");
			stringall2 = stringall2.concat(arrLine[m]);
		}
		Fieldconcatenated2.set(stringall2);
		
		// So write another key_value for t+1
		context.write(new IntWritable(Timesec+1), Fieldconcatenated2);
		// Later inreducer, this second text line will be consolidated to t+1
	}

}
