import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TimeJoinDriver2 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:54310");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName() );
		
		Job job = Job.getInstance(conf, "Join the same table by time");
		job.setJarByClass(TimeJoinDriver2.class);
		
		job.setMapperClass(TimeJoinMapper2.class);
		
		job.setReducerClass(TimeJoinReducer2.class);

		// specify output types
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/hduser/ethylene_CO_seconds"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/hduser/ethylene_CO_seconds_timejoin2"));
		
		if (!job.waitForCompletion(true))
			return;
	}

}
