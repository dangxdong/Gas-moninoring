import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class COtoSecondDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:54310");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName() );
		
		Job job = Job.getInstance(conf, "CO into fewer rows per second");
		job.setJarByClass(COtoSecondDriver.class);
		
		job.setMapperClass(COtoSecondMapper.class);
		
		job.setReducerClass(COtoSecondReducer.class);

		// specify output types
		// Have to specify the output types for the mapper  !!!!
		// if they are not the same as the output types of the reducer
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);


		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/hduser/ethylene_CO"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/hduser/ethylene_CO_seconds"));
		
		if (!job.waitForCompletion(true))
			return;
	}

}
