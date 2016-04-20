import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

public class TimeJoinDriverHbase {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:54310");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName() );

		Job job = Job.getInstance(conf, "Join the same table by time");
		job.setJarByClass(TimeJoinDriverHbase.class);
		job.setMapperClass(TimeJoinMapperHbase.class);
		job.setReducerClass(TimeJoinReducerHbase.class);

		// specify Mapper output types
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/hduser/ethylene_CO_seconds"));
		
		// specify the target in hbase:
		TableMapReduceUtil.initTableReducerJob("ethylene_CO_seconds_timejoin", TimeJoinReducerHbase.class, job);

		job.setNumReduceTasks(1);  // don't know what this affects
		
		if (!job.waitForCompletion(true))
			return;
	}

}
