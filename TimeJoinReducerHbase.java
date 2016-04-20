import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;


public class TimeJoinReducerHbase extends TableReducer<IntWritable, Text, ImmutableBytesWritable> {

	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		int Timesecint = key.get();
		// only write out if key is not zero, that is, only start form timesec=1
		if (Timesecint > 0) {
			String arrLine1 = "";
			String arrLine2 = "";
			for (Text val : values) {
				String[] arrLine = val.toString().split(",");
				int len = arrLine.length;
				// if length is 19, should be on the left,
				// else length is 18, should be on the right.
				if (len == 19) {
					arrLine1 = val.toString();
				}
				else {
					arrLine2 = val.toString();
				}
			}
	        // after loop:
			// At most there will be two rounds of loop for each key.
			// the situation when key=0 is already excluded.
			// need to also exclude when the key is the biggest n+1,
			// where there is only arrLine2 for the key, but no arrLine1

			if (arrLine1.length() > 0) {
				String arrLine12 = arrLine1.concat(",");
				arrLine12 = arrLine12.concat(arrLine2);
				
				// In Hbase, create the target table before running this job:
				// create 'ethylene_CO_seconds_timejoin', 'time', 'chemical_conc', 'sensor_read'
				
				String[] arrLine12arr = arrLine12.split(",");
				// so arrLine12arr is an array of strings
				// with 3+16+16 fields.
				
				
				Put put = new Put(Bytes.toBytes(key.toString()));
				
				put.addColumn("time".getBytes(), "Timesec".getBytes(), Bytes.toBytes(arrLine12arr[0]));
				put.addColumn("chemical_conc".getBytes(), "CO".getBytes(), Bytes.toBytes(arrLine12arr[1]));
				put.addColumn("chemical_conc".getBytes(), "Ethylene".getBytes(), Bytes.toBytes(arrLine12arr[2]));
				
				put.addColumn("sensor_read".getBytes(), "sr1".getBytes(), Bytes.toBytes(arrLine12arr[3]));
				put.addColumn("sensor_read".getBytes(), "sr2".getBytes(), Bytes.toBytes(arrLine12arr[4]));
				put.addColumn("sensor_read".getBytes(), "sr3".getBytes(), Bytes.toBytes(arrLine12arr[5]));
				put.addColumn("sensor_read".getBytes(), "sr4".getBytes(), Bytes.toBytes(arrLine12arr[6]));
				put.addColumn("sensor_read".getBytes(), "sr5".getBytes(), Bytes.toBytes(arrLine12arr[7]));
				put.addColumn("sensor_read".getBytes(), "sr6".getBytes(), Bytes.toBytes(arrLine12arr[8]));
				put.addColumn("sensor_read".getBytes(), "sr7".getBytes(), Bytes.toBytes(arrLine12arr[9]));
				put.addColumn("sensor_read".getBytes(), "sr8".getBytes(), Bytes.toBytes(arrLine12arr[10]));
				put.addColumn("sensor_read".getBytes(), "sr9".getBytes(), Bytes.toBytes(arrLine12arr[11]));
				put.addColumn("sensor_read".getBytes(), "sr10".getBytes(), Bytes.toBytes(arrLine12arr[12]));
				put.addColumn("sensor_read".getBytes(), "sr11".getBytes(), Bytes.toBytes(arrLine12arr[13]));
				put.addColumn("sensor_read".getBytes(), "sr12".getBytes(), Bytes.toBytes(arrLine12arr[14]));
				put.addColumn("sensor_read".getBytes(), "sr13".getBytes(), Bytes.toBytes(arrLine12arr[15]));
				put.addColumn("sensor_read".getBytes(), "sr14".getBytes(), Bytes.toBytes(arrLine12arr[16]));
				put.addColumn("sensor_read".getBytes(), "sr15".getBytes(), Bytes.toBytes(arrLine12arr[17]));
				put.addColumn("sensor_read".getBytes(), "sr16".getBytes(), Bytes.toBytes(arrLine12arr[18]));
				put.addColumn("sensor_read".getBytes(), "sr1p".getBytes(), Bytes.toBytes(arrLine12arr[19]));
				put.addColumn("sensor_read".getBytes(), "sr2p".getBytes(), Bytes.toBytes(arrLine12arr[20]));
				put.addColumn("sensor_read".getBytes(), "sr3p".getBytes(), Bytes.toBytes(arrLine12arr[21]));
				put.addColumn("sensor_read".getBytes(), "sr4p".getBytes(), Bytes.toBytes(arrLine12arr[22]));
				put.addColumn("sensor_read".getBytes(), "sr5p".getBytes(), Bytes.toBytes(arrLine12arr[23]));
				put.addColumn("sensor_read".getBytes(), "sr6p".getBytes(), Bytes.toBytes(arrLine12arr[24]));
				put.addColumn("sensor_read".getBytes(), "sr7p".getBytes(), Bytes.toBytes(arrLine12arr[25]));
				put.addColumn("sensor_read".getBytes(), "sr8p".getBytes(), Bytes.toBytes(arrLine12arr[26]));
				put.addColumn("sensor_read".getBytes(), "sr9p".getBytes(), Bytes.toBytes(arrLine12arr[27]));
				put.addColumn("sensor_read".getBytes(), "sr10p".getBytes(), Bytes.toBytes(arrLine12arr[28]));
				put.addColumn("sensor_read".getBytes(), "sr11p".getBytes(), Bytes.toBytes(arrLine12arr[29]));
				put.addColumn("sensor_read".getBytes(), "sr12p".getBytes(), Bytes.toBytes(arrLine12arr[30]));
				put.addColumn("sensor_read".getBytes(), "sr13p".getBytes(), Bytes.toBytes(arrLine12arr[31]));
				put.addColumn("sensor_read".getBytes(), "sr14p".getBytes(), Bytes.toBytes(arrLine12arr[32]));
				put.addColumn("sensor_read".getBytes(), "sr15p".getBytes(), Bytes.toBytes(arrLine12arr[33]));
				put.addColumn("sensor_read".getBytes(), "sr16p".getBytes(), Bytes.toBytes(arrLine12arr[34]));
				
				context.write(null, put);
			}
			// otherwise, do nothing.	
		}
		// otherwise, do nothing.
	}

}
